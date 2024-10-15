package com.getindata.connectors.http.internal.sink;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.http.Fault;
import com.github.tomakehurst.wiremock.stubbing.ServeEvent;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MetricOptions;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Metric;
import org.apache.flink.metrics.MetricConfig;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.reporter.MetricReporter;
import org.apache.flink.metrics.reporter.MetricReporterFactory;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.testutils.junit.extensions.ContextClassLoaderExtension;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static com.github.tomakehurst.wiremock.stubbing.Scenario.STARTED;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.getindata.connectors.http.HttpSink;
import com.getindata.connectors.http.internal.config.HttpConnectorConfigConstants;
import com.getindata.connectors.http.internal.config.SinkRequestSubmitMode;
import com.getindata.connectors.http.internal.sink.httpclient.JavaNetSinkHttpClient;

public class HttpSinkConnectionTest {

    @RegisterExtension
    static final ContextClassLoaderExtension CONTEXT_CLASS_LOADER_EXTENSION =
            ContextClassLoaderExtension.builder()
                    .withServiceEntry(
                            MetricReporterFactory.class,
                            SendErrorsTestReporterFactory.class.getName())
                    .build();


    private static final int SERVER_PORT = 9090;

    private static final int HTTPS_SERVER_PORT = 8443;

    private static final Set<Integer> messageIds = IntStream.range(0, 50)
            .boxed()
            .collect(Collectors.toSet());

    private static final List<String> messages = messageIds.stream()
            .map(i -> "{\"http-sink-id\":" + i + "}")
            .collect(Collectors.toList());

    private StreamExecutionEnvironment env;

    private WireMockServer wireMockServer;

    @BeforeEach
    public void setUp() {
        SendErrorsTestReporterFactory.reset();

        env = StreamExecutionEnvironment.getExecutionEnvironment(new Configuration() {
            {
                setString(
                        ConfigConstants.METRICS_REPORTER_PREFIX
                                + "test."
                                + MetricOptions.REPORTER_FACTORY_CLASS.key(),
                        SendErrorsTestReporterFactory.class.getName());
            }
        });

        wireMockServer = new WireMockServer(SERVER_PORT, HTTPS_SERVER_PORT);
        wireMockServer.start();
    }

    @AfterEach
    public void tearDown() {
        wireMockServer.stop();
    }

    @Test
    public void testConnection_singleRequestMode() throws Exception {

        @SuppressWarnings("unchecked")
        Function<ServeEvent, Map<Object, Object>> responseMapper = response -> {
            try {
                return new ObjectMapper().readValue(response.getRequest().getBody(), HashMap.class);
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

        Function<ServeEvent, List<Map<Object, Object>>> responseMapper = response -> {
            try {
                return new ObjectMapper().readValue(response.getRequest().getBody(),
                        new TypeReference<List<Map<Object, Object>>>(){});
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
            SinkRequestSubmitMode mode,
            Function<? super ServeEvent, T> responseMapper) throws Exception {

        String endpoint = "/myendpoint";
        String contentTypeHeader = "application/json";

        wireMockServer.stubFor(any(urlPathEqualTo(endpoint))
                .withHeader("Content-Type", equalTo(contentTypeHeader))
                .willReturn(
                        aResponse().withHeader("Content-Type", contentTypeHeader)
                                .withStatus(200)
                                .withBody("{}")));

        var source = env.fromCollection(messages);
        var httpSink = HttpSink.<String>builder()
                .setEndpointUrl("http://localhost:" + SERVER_PORT + endpoint)
                .setElementConverter(
                    (s, _context) ->
                    new HttpSinkRequestEntry("POST", s.getBytes(StandardCharsets.UTF_8)))
                .setSinkHttpClientBuilder(JavaNetSinkHttpClient::new)
                .setProperty(
                        HttpConnectorConfigConstants.SINK_HEADER_PREFIX + "Content-Type",
                        contentTypeHeader)
                .setProperty(
                        HttpConnectorConfigConstants.SINK_HTTP_REQUEST_MODE,
                        mode.getMode()
                )
                .build();
        source.sinkTo(httpSink);
        env.execute("Http Sink test connection");

        var responses = wireMockServer.getAllServeEvents();
        assertTrue(responses.stream()
                .allMatch(response -> Objects.equals(response.getRequest().getUrl(), endpoint)));
        assertTrue(
                responses.stream().allMatch(response -> response.getResponse().getStatus() == 200));
        assertTrue(responses.stream()
                .allMatch(response -> Objects.equals(response.getRequest().getUrl(), endpoint)));
        assertTrue(
                responses.stream().allMatch(response -> response.getResponse().getStatus() == 200));

        List<T> collect = responses.stream().map(responseMapper).collect(Collectors.toList());
        assertTrue(collect.stream().allMatch(Objects::nonNull));
        return collect;
    }

    @Test
    public void testServerErrorConnection() throws Exception {
        wireMockServer.stubFor(any(urlPathEqualTo("/myendpoint"))
                .withHeader("Content-Type", equalTo("application/json"))
                .inScenario("Retry Scenario")
                .whenScenarioStateIs(STARTED)
                .willReturn(serverError())
                .willSetStateTo("Cause Success"));
        wireMockServer.stubFor(any(urlPathEqualTo("/myendpoint"))
                .withHeader("Content-Type", equalTo("application/json"))
                .inScenario("Retry Scenario")
                .whenScenarioStateIs("Cause Success")
                .willReturn(aResponse().withStatus(200))
                .willSetStateTo("Cause Success"));

        var source = env.fromCollection(List.of(messages.get(0)));
        var httpSink = HttpSink.<String>builder()
                .setEndpointUrl("http://localhost:" + SERVER_PORT + "/myendpoint")
                .setElementConverter(
                    (s, _context) ->
                    new HttpSinkRequestEntry("POST", s.getBytes(StandardCharsets.UTF_8)))
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
        wireMockServer.stubFor(any(urlPathEqualTo("/myendpoint"))
                .withHeader("Content-Type", equalTo("application/json"))
                .inScenario("Retry Scenario")
                .whenScenarioStateIs(STARTED)
                .willReturn(aResponse().withFault(Fault.EMPTY_RESPONSE))
                .willSetStateTo("Cause Success"));

        wireMockServer.stubFor(any(urlPathEqualTo("/myendpoint"))
                .withHeader("Content-Type", equalTo("application/json"))
                .inScenario("Retry Scenario")
                .whenScenarioStateIs("Cause Success")
                .willReturn(aResponse().withStatus(200))
                .willSetStateTo("Cause Success"));

        var source = env.fromCollection(List.of(messages.get(0)));
        var httpSink = HttpSink.<String>builder()
                .setEndpointUrl("http://localhost:" + SERVER_PORT + "/myendpoint")
                .setElementConverter(
                    (s, _context) ->
                    new HttpSinkRequestEntry("POST",
                            s.getBytes(StandardCharsets.UTF_8)))
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
    public void testFailedConnection404OnWhiteList() throws Exception {
        wireMockServer.stubFor(any(urlPathEqualTo("/myendpoint"))
                .withHeader("Content-Type", equalTo("application/json"))
                .willReturn(aResponse().withBody("404 body").withStatus(404)));

        var source = env.fromCollection(List.of(messages.get(0)));
        var httpSink = HttpSink.<String>builder()
                .setEndpointUrl("http://localhost:" + SERVER_PORT + "/myendpoint")
                .setElementConverter(
                    (s, _context) ->
                    new HttpSinkRequestEntry("POST", s.getBytes(StandardCharsets.UTF_8)))
                .setSinkHttpClientBuilder(JavaNetSinkHttpClient::new)
                .setProperty("gid.connector.http.sink.error.code.exclude", "404, 405")
                .setProperty("gid.connector.http.sink.error.code", "4XX")
                .build();
        source.sinkTo(httpSink);
        env.execute("Http Sink test failed connection");

        assertEquals(0, SendErrorsTestReporterFactory.getCount());
    }

    // must be public because of the reflection
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
        public void open(MetricConfig metricConfig) {
        }

        @Override
        public void close() {
        }

        @Override
        public void notifyOfAddedMetric(
                Metric metric,
                String s,
                MetricGroup metricGroup) {

            if ("numRecordsSendErrors".equals(s)) {
                numRecordsSendErrors.add((Counter) metric);
            }
        }

        @Override
        public void notifyOfRemovedMetric(Metric metric, String s, MetricGroup metricGroup) {
        }

        @Override
        public MetricReporter createMetricReporter(Properties properties) {
            return new SendErrorsTestReporterFactory();
        }
    }
}
