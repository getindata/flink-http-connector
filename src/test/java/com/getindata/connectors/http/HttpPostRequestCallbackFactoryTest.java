package com.getindata.connectors.http;

import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import com.github.tomakehurst.wiremock.WireMockServer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.getindata.connectors.http.internal.sink.httpclient.HttpRequest;
import com.getindata.connectors.http.internal.table.lookup.HttpLookupSourceRequestEntry;
import com.getindata.connectors.http.internal.table.sink.HttpDynamicTableSinkFactory;
import static com.getindata.connectors.http.TestLookupPostRequestCallbackFactory.TEST_LOOKUP_POST_REQUEST_CALLBACK_IDENT;
import static com.getindata.connectors.http.TestPostRequestCallbackFactory.TEST_POST_REQUEST_CALLBACK_IDENT;

public class HttpPostRequestCallbackFactoryTest {
    private static final int SERVER_PORT = 9090;

    private WireMockServer wireMockServer;
    protected StreamExecutionEnvironment env;
    protected StreamTableEnvironment tEnv;

    private static final ArrayList<HttpRequest> requestEntries = new ArrayList<>();

    private static final ArrayList<HttpLookupSourceRequestEntry>
            lookupRequestEntries = new ArrayList<>();

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
                + "  'gid.connector.http.sink.request-callback' = '%s',\n"
                + "  'gid.connector.http.sink.writer.request.mode' = '%s',\n"
                + "  'gid.connector.http.sink.header.Content-Type' = 'application/json'\n"
                + ")",
                HttpDynamicTableSinkFactory.IDENTIFIER,
                "http://localhost:" + SERVER_PORT + "/myendpoint",
                TEST_POST_REQUEST_CALLBACK_IDENT,
                mode
            );
        tEnv.executeSql(createTable);

        final String insert = "INSERT INTO http VALUES (1)";
        tEnv.executeSql(insert).await();

        assertEquals(1, requestEntries.size());
        assertEquals(1, responses.size());

        String actualRequest = requestEntries.get(0).getElements().stream()
            .map(element -> new String(element, StandardCharsets.UTF_8))
            .collect(Collectors.joining());

        Assertions.assertThat(actualRequest).isEqualToIgnoringNewLines(expectedRequest);
    }

    @Test
    public void httpLookupPostRequestCallbackFactoryTest()
            throws ExecutionException, InterruptedException {
        wireMockServer.stubFor(any(urlPathEqualTo("/myendpoint")).willReturn(
                aResponse().withStatus(200).withBody("{\"customerId\": 1}")
        ));

        final String createTable1 =
                "CREATE TABLE Orders (\n" +
                        "    proc_time AS PROCTIME(),\n" +
                        "    orderId INT\n" +
                        ") WITH (\n" +
                        "  'connector' = 'datagen',\n" +
                        "  'fields.orderId.kind' = 'sequence',\n" +
                        "  'fields.orderId.start' = '1',\n" +
                        "  'fields.orderId.end' = '1'\n" +
                        ");";
        tEnv.executeSql(createTable1);

        final String createTable2 =
                String.format(
                        "CREATE TABLE Customers (\n"
                                + "  `customerId` INT\n"
                                + ") with (\n"
                                + "  'connector' = '%s',\n"
                                + "  'url' = '%s',\n"
                                + "  'format' = 'json',\n"
                                + "  'gid.connector.http.source.lookup.request-callback' = '%s'\n"
                                + ")",
                        "rest-lookup",
                        "http://localhost:" + SERVER_PORT + "/myendpoint",
                        TEST_LOOKUP_POST_REQUEST_CALLBACK_IDENT
                );
        tEnv.executeSql(createTable2);

        final String joinTable =
                "SELECT o.`orderId`, c.`customerId`\n" +
                        "    FROM Orders AS o\n" +
                        "    JOIN Customers FOR SYSTEM_TIME AS OF o.`proc_time` AS c\n" +
                        "    ON o.`orderId` = c.`customerId`;";

        final TableResult resultTable = tEnv.sqlQuery(joinTable).execute();
        resultTable.await();

        assertEquals(1, lookupRequestEntries.size());
        assertEquals(1, responses.size());
    }

    public static class TestPostRequestCallback implements HttpPostRequestCallback<HttpRequest> {
        @Override
        public void call(
            HttpResponse<String> response,
            HttpRequest requestEntry,
            String endpointUrl,
            Map<String, String> headerMap
        ) {
            requestEntries.add(requestEntry);
            responses.add(response);
        }
    }

    public static class TestLookupPostRequestCallback
            implements HttpPostRequestCallback<HttpLookupSourceRequestEntry> {
        @Override
        public void call(
                HttpResponse<String> response,
                HttpLookupSourceRequestEntry requestEntry,
                String endpointUrl,
                Map<String, String> headerMap
        ) {
            lookupRequestEntries.add(requestEntry);
            responses.add(response);
        }
    }
}
