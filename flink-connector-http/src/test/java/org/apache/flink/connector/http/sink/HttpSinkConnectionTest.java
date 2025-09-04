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

package org.apache.flink.connector.http.sink;

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MetricOptions;
import org.apache.flink.connector.http.HttpSink;
import org.apache.flink.connector.http.WireMockServerPortAllocator;
import org.apache.flink.connector.http.config.HttpConnectorConfigConstants;
import org.apache.flink.connector.http.config.SinkRequestSubmitMode;
import org.apache.flink.connector.http.sink.httpclient.JavaNetSinkHttpClient;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Metric;
import org.apache.flink.metrics.MetricConfig;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.reporter.MetricReporter;
import org.apache.flink.metrics.reporter.MetricReporterFactory;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.testutils.junit.extensions.ContextClassLoaderExtension;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.type.TypeReference;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.http.Fault;
import com.github.tomakehurst.wiremock.stubbing.ServeEvent;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.any;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.serverError;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo;
import static com.github.tomakehurst.wiremock.stubbing.Scenario.STARTED;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Test for {@link HttpSink }. */
public class HttpSinkConnectionTest {

    @RegisterExtension
    static final ContextClassLoaderExtension CONTEXT_CLASS_LOADER_EXTENSION =
            ContextClassLoaderExtension.builder()
                    .withServiceEntry(
                            MetricReporterFactory.class,
                            SendErrorsTestReporterFactory.class.getName())
                    .build();

    private static int serverPort;

    private static int secServerPort;

    private static final Set<Integer> messageIds =
            IntStream.range(0, 50).boxed().collect(Collectors.toSet());

    private static final List<String> messages =
            messageIds.stream()
                    .map(i -> "{\"http-sink-id\":" + i + "}")
                    .collect(Collectors.toList());

    private StreamExecutionEnvironment env;

    private WireMockServer wireMockServer;

    @BeforeEach
    public void setUp() {
        SendErrorsTestReporterFactory.reset();
        serverPort = WireMockServerPortAllocator.getServerPort();
        secServerPort = WireMockServerPortAllocator.getSecureServerPort();

        env =
                StreamExecutionEnvironment.getExecutionEnvironment(
                        new Configuration() {
                            {
                                setString(
                                        ConfigConstants.METRICS_REPORTER_PREFIX
                                                + "test."
                                                + MetricOptions.REPORTER_FACTORY_CLASS.key(),
                                        SendErrorsTestReporterFactory.class.getName());
                            }
                        });

        wireMockServer = new WireMockServer(serverPort, secServerPort);
        wireMockServer.start();
    }

    @AfterEach
    public void tearDown() {
        wireMockServer.stop();
    }

    @Test
    public void testConnection_singleRequestMode() throws Exception {

        @SuppressWarnings("unchecked")
        Function<ServeEvent, Map<Object, Object>> responseMapper =
                response -> {
                    try {
                        return new ObjectMapper()
                                .readValue(response.getRequest().getBody(), HashMap.class);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                };

        List<Map<Object, Object>> responses =
                testConnection(SinkRequestSubmitMode.SINGLE, responseMapper);

        var idsSet = new HashSet<>(messageIds);
        for (var request : responses) {
            var el = (Integer) request.get("http-sink-id");
            assertTrue(idsSet.contains(el));
            idsSet.remove(el);
        }

        // check that we hot responses for all requests.
        assertTrue(idsSet.isEmpty());
    }

    @Test
    public void testConnection_batchRequestMode() throws Exception {

        Function<ServeEvent, List<Map<Object, Object>>> responseMapper =
                response -> {
                    try {
                        return new ObjectMapper()
                                .readValue(
                                        response.getRequest().getBody(),
                                        new TypeReference<List<Map<Object, Object>>>() {});
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                };

        List<List<Map<Object, Object>>> responses =
                testConnection(SinkRequestSubmitMode.BATCH, responseMapper);

        var idsSet = new HashSet<>(messageIds);
        for (var requests : responses) {
            for (var request : requests) {
                var el = (Integer) request.get("http-sink-id");
                assertTrue(idsSet.contains(el));
                idsSet.remove(el);
            }
        }

        // check that we hot responses for all requests.
        assertTrue(idsSet.isEmpty());
    }

    public <T> List<T> testConnection(
            SinkRequestSubmitMode mode, Function<? super ServeEvent, T> responseMapper)
            throws Exception {

        String endpoint = "/myendpoint";
        String contentTypeHeader = "application/json";

        wireMockServer.stubFor(
                any(urlPathEqualTo(endpoint))
                        .withHeader("Content-Type", equalTo(contentTypeHeader))
                        .willReturn(
                                aResponse()
                                        .withHeader("Content-Type", contentTypeHeader)
                                        .withStatus(200)
                                        .withBody("{}")));

        var source = env.fromCollection(messages);
        var httpSink =
                HttpSink.<String>builder()
                        .setEndpointUrl("http://localhost:" + serverPort + endpoint)
                        .setElementConverter(
                                (s, _context) ->
                                        new HttpSinkRequestEntry(
                                                "POST", s.getBytes(StandardCharsets.UTF_8)))
                        .setSinkHttpClientBuilder(JavaNetSinkHttpClient::new)
                        .setProperty(
                                HttpConnectorConfigConstants.SINK_HEADER_PREFIX + "Content-Type",
                                contentTypeHeader)
                        .setProperty(
                                HttpConnectorConfigConstants.SINK_HTTP_REQUEST_MODE, mode.getMode())
                        .build();
        source.sinkTo(httpSink);
        env.execute("Http Sink test connection");

        var responses = wireMockServer.getAllServeEvents();
        assertTrue(
                responses.stream()
                        .allMatch(
                                response ->
                                        Objects.equals(response.getRequest().getUrl(), endpoint)));
        assertTrue(
                responses.stream().allMatch(response -> response.getResponse().getStatus() == 200));
        assertTrue(
                responses.stream()
                        .allMatch(
                                response ->
                                        Objects.equals(response.getRequest().getUrl(), endpoint)));
        assertTrue(
                responses.stream().allMatch(response -> response.getResponse().getStatus() == 200));

        List<T> collect = responses.stream().map(responseMapper).collect(Collectors.toList());
        assertTrue(collect.stream().allMatch(Objects::nonNull));
        return collect;
    }

    @Test
    public void testServerErrorConnection() throws Exception {
        wireMockServer.stubFor(
                any(urlPathEqualTo("/myendpoint"))
                        .withHeader("Content-Type", equalTo("application/json"))
                        .inScenario("Retry Scenario")
                        .whenScenarioStateIs(STARTED)
                        .willReturn(serverError())
                        .willSetStateTo("Cause Success"));
        wireMockServer.stubFor(
                any(urlPathEqualTo("/myendpoint"))
                        .withHeader("Content-Type", equalTo("application/json"))
                        .inScenario("Retry Scenario")
                        .whenScenarioStateIs("Cause Success")
                        .willReturn(aResponse().withStatus(200))
                        .willSetStateTo("Cause Success"));

        var source = env.fromCollection(List.of(messages.get(0)));
        var httpSink =
                HttpSink.<String>builder()
                        .setEndpointUrl("http://localhost:" + serverPort + "/myendpoint")
                        .setElementConverter(
                                (s, _context) ->
                                        new HttpSinkRequestEntry(
                                                "POST", s.getBytes(StandardCharsets.UTF_8)))
                        .setSinkHttpClientBuilder(JavaNetSinkHttpClient::new)
                        .build();
        source.sinkTo(httpSink);
        env.execute("Http Sink test failed connection");

        assertEquals(1, SendErrorsTestReporterFactory.getCount());
        // TODO: reintroduce along with the retries
        //  var postedRequests = wireMockServer
        //  .findAll(postRequestedFor(urlPathEqualTo("/myendpoint")));
        //  assertEquals(2, postedRequests.size());
        //  assertEquals(postedRequests.get(0).getBodyAsString(),
        //  postedRequests.get(1).getBodyAsString());
    }

    @Test
    public void testFailedConnection() throws Exception {
        wireMockServer.stubFor(
                any(urlPathEqualTo("/myendpoint"))
                        .withHeader("Content-Type", equalTo("application/json"))
                        .inScenario("Retry Scenario")
                        .whenScenarioStateIs(STARTED)
                        .willReturn(aResponse().withFault(Fault.EMPTY_RESPONSE))
                        .willSetStateTo("Cause Success"));

        wireMockServer.stubFor(
                any(urlPathEqualTo("/myendpoint"))
                        .withHeader("Content-Type", equalTo("application/json"))
                        .inScenario("Retry Scenario")
                        .whenScenarioStateIs("Cause Success")
                        .willReturn(aResponse().withStatus(200))
                        .willSetStateTo("Cause Success"));

        var source = env.fromCollection(List.of(messages.get(0)));
        var httpSink =
                HttpSink.<String>builder()
                        .setEndpointUrl("http://localhost:" + serverPort + "/myendpoint")
                        .setElementConverter(
                                (s, _context) ->
                                        new HttpSinkRequestEntry(
                                                "POST", s.getBytes(StandardCharsets.UTF_8)))
                        .setSinkHttpClientBuilder(JavaNetSinkHttpClient::new)
                        .build();
        source.sinkTo(httpSink);
        env.execute("Http Sink test failed connection");

        assertEquals(1, SendErrorsTestReporterFactory.getCount());
        // var postedRequests = wireMockServer
        // .findAll(postRequestedFor(urlPathEqualTo("/myendpoint")));
        // assertEquals(2, postedRequests.size());
        // assertEquals(postedRequests.get(0).getBodyAsString(),
        // postedRequests.get(1).getBodyAsString());
    }

    @Test
    public void testFailedConnection404OnIncludeList() throws Exception {
        wireMockServer.stubFor(
                any(urlPathEqualTo("/myendpoint"))
                        .withHeader("Content-Type", equalTo("application/json"))
                        .willReturn(aResponse().withBody("404 body").withStatus(404)));

        var source = env.fromCollection(List.of(messages.get(0)));
        var httpSink =
                HttpSink.<String>builder()
                        .setEndpointUrl("http://localhost:" + serverPort + "/myendpoint")
                        .setElementConverter(
                                (s, _context) ->
                                        new HttpSinkRequestEntry(
                                                "POST", s.getBytes(StandardCharsets.UTF_8)))
                        .setSinkHttpClientBuilder(JavaNetSinkHttpClient::new)
                        .setProperty("http.sink.error.code.exclude", "404, 405")
                        .setProperty("http.sink.error.code", "4XX")
                        .build();
        source.sinkTo(httpSink);
        env.execute("Http Sink test failed connection");

        assertEquals(0, SendErrorsTestReporterFactory.getCount());
    }

    /** must be public because of the reflection. */
    public static class SendErrorsTestReporterFactory
            implements MetricReporter, MetricReporterFactory {
        static volatile List<Counter> numRecordsSendErrors = null;

        public static long getCount() {
            return numRecordsSendErrors.stream().map(Counter::getCount).reduce(0L, Long::sum);
        }

        public static void reset() {
            numRecordsSendErrors = new ArrayList<>();
        }

        @Override
        public void open(MetricConfig metricConfig) {}

        @Override
        public void close() {}

        @Override
        public void notifyOfAddedMetric(Metric metric, String s, MetricGroup metricGroup) {

            if ("numRecordsSendErrors".equals(s)) {
                numRecordsSendErrors.add((Counter) metric);
            }
        }

        @Override
        public void notifyOfRemovedMetric(Metric metric, String s, MetricGroup metricGroup) {}

        @Override
        public MetricReporter createMetricReporter(Properties properties) {
            return new SendErrorsTestReporterFactory();
        }
    }
}
