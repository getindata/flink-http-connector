/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.http;

import org.apache.flink.connector.http.sink.httpclient.HttpRequest;
import org.apache.flink.connector.http.table.lookup.HttpLookupSourceRequestEntry;
import org.apache.flink.connector.http.table.sink.HttpDynamicTableSinkFactory;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import com.github.tomakehurst.wiremock.WireMockServer;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.any;
import static com.github.tomakehurst.wiremock.client.WireMock.ok;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo;
import static org.apache.flink.connector.http.TestLookupPostRequestCallbackFactory.TEST_LOOKUP_POST_REQUEST_CALLBACK_IDENT;
import static org.apache.flink.connector.http.TestPostRequestCallbackFactory.TEST_POST_REQUEST_CALLBACK_IDENT;
import static org.junit.jupiter.api.Assertions.assertEquals;

/** Test for {@link HttpPostRequestCallbackFactory}. */
public class HttpPostRequestCallbackFactoryTest {
    private static final int SERVER_PORT = WireMockServerPortAllocator.getServerPort();

    private WireMockServer wireMockServer;
    protected StreamExecutionEnvironment env;
    protected StreamTableEnvironment tEnv;

    private static final ArrayList<HttpRequest> requestEntries = new ArrayList<>();

    private static final ArrayList<HttpLookupSourceRequestEntry> lookupRequestEntries =
            new ArrayList<>();

    private static final ArrayList<HttpResponse<String>> responses = new ArrayList<>();

    @BeforeEach
    public void setup() {
        wireMockServer = new WireMockServer(SERVER_PORT);
        wireMockServer.start();

        env = StreamExecutionEnvironment.getExecutionEnvironment();
        tEnv = StreamTableEnvironment.create(env);

        requestEntries.clear();
        responses.clear();
    }

    @AfterEach
    public void tearDown() {
        wireMockServer.stop();
    }

    @ParameterizedTest
    @CsvSource(value = {"single, {\"id\":1}", "batch, [{\"id\":1}]"})
    public void httpPostRequestCallbackFactoryTest(String mode, String expectedRequest)
            throws ExecutionException, InterruptedException {
        wireMockServer.stubFor(any(urlPathEqualTo("/myendpoint")).willReturn(ok()));

        final String createTable =
                String.format(
                        "CREATE TABLE http (\n"
                                + "  id bigint\n"
                                + ") with (\n"
                                + "  'connector' = '%s',\n"
                                + "  'url' = '%s',\n"
                                + "  'format' = 'json',\n"
                                + "  'http.sink.request-callback' = '%s',\n"
                                + "  'http.sink.writer.request.mode' = '%s',\n"
                                + "  'http.sink.header.Content-Type' = 'application/json'\n"
                                + ")",
                        HttpDynamicTableSinkFactory.IDENTIFIER,
                        "http://localhost:" + SERVER_PORT + "/myendpoint",
                        TEST_POST_REQUEST_CALLBACK_IDENT,
                        mode);
        tEnv.executeSql(createTable);

        final String insert = "INSERT INTO http VALUES (1)";
        tEnv.executeSql(insert).await();

        assertEquals(1, requestEntries.size());
        assertEquals(1, responses.size());

        String actualRequest =
                requestEntries.get(0).getElements().stream()
                        .map(element -> new String(element, StandardCharsets.UTF_8))
                        .collect(Collectors.joining());

        Assertions.assertThat(actualRequest).isEqualToIgnoringNewLines(expectedRequest);
    }

    @Test
    public void httpLookupPostRequestCallbackFactoryTest()
            throws ExecutionException, InterruptedException {
        wireMockServer.stubFor(
                any(urlPathEqualTo("/myendpoint"))
                        .willReturn(aResponse().withStatus(200).withBody("{\"customerId\": 1}")));

        final String createTable1 =
                "CREATE TABLE Orders (\n"
                        + "    proc_time AS PROCTIME(),\n"
                        + "    orderId INT\n"
                        + ") WITH (\n"
                        + "  'connector' = 'datagen',\n"
                        + "  'fields.orderId.kind' = 'sequence',\n"
                        + "  'fields.orderId.start' = '1',\n"
                        + "  'fields.orderId.end' = '1'\n"
                        + ");";
        tEnv.executeSql(createTable1);

        final String createTable2 =
                String.format(
                        "CREATE TABLE Customers (\n"
                                + "  `customerId` INT\n"
                                + ") with (\n"
                                + "  'connector' = '%s',\n"
                                + "  'url' = '%s',\n"
                                + "  'format' = 'json',\n"
                                + "  'http.source.lookup.request-callback' = '%s'\n"
                                + ")",
                        "rest-lookup",
                        "http://localhost:" + SERVER_PORT + "/myendpoint",
                        TEST_LOOKUP_POST_REQUEST_CALLBACK_IDENT);
        tEnv.executeSql(createTable2);

        final String joinTable =
                "SELECT o.`orderId`, c.`customerId`\n"
                        + "    FROM Orders AS o\n"
                        + "    JOIN Customers FOR SYSTEM_TIME AS OF o.`proc_time` AS c\n"
                        + "    ON o.`orderId` = c.`customerId`;";

        final TableResult resultTable = tEnv.sqlQuery(joinTable).execute();
        resultTable.await();

        assertEquals(1, lookupRequestEntries.size());
        assertEquals(1, responses.size());
    }

    /** TestPostRequestCallback. */
    public static class TestPostRequestCallback implements HttpPostRequestCallback<HttpRequest> {
        @Override
        public void call(
                HttpResponse<String> response,
                HttpRequest requestEntry,
                String endpointUrl,
                Map<String, String> headerMap) {
            requestEntries.add(requestEntry);
            responses.add(response);
        }
    }

    /** TestLookupPostRequestCallback. */
    public static class TestLookupPostRequestCallback
            implements HttpPostRequestCallback<HttpLookupSourceRequestEntry> {
        @Override
        public void call(
                HttpResponse<String> response,
                HttpLookupSourceRequestEntry requestEntry,
                String endpointUrl,
                Map<String, String> headerMap) {
            lookupRequestEntries.add(requestEntry);
            responses.add(response);
        }
    }
}
