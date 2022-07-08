package com.getindata.connectors.http.sink;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.getindata.connectors.http.HttpSink;
import com.getindata.connectors.http.internal.sink.HttpSinkRequestEntry;
import com.getindata.connectors.http.internal.sink.httpclient.JavaNetSinkHttpClient;
import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.http.Fault;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Metric;
import org.apache.flink.metrics.MetricConfig;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.reporter.MetricReporter;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static com.github.tomakehurst.wiremock.stubbing.Scenario.STARTED;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class HttpSinkConnectionTest {
  private static final int SERVER_PORT = 9090;
  private static final Set<Integer> messageIds = IntStream.range(0, 50).boxed().collect(Collectors.toSet());
  private static final List<String> messages =
      messageIds.stream().map(i -> "{\"http-sink-id\":" + i + "}").collect(Collectors.toList());

  private StreamExecutionEnvironment env;
  private WireMockServer wireMockServer;

  // must be public because of the reflection
  public static class SendErrorsTestReporter implements MetricReporter {
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
        Metric metric, String s, MetricGroup metricGroup
    ) {
      if ("numRecordsSendErrors".equals(s)) {
        numRecordsSendErrors.add((Counter) metric);
      }
    }

    @Override
    public void notifyOfRemovedMetric(Metric metric, String s, MetricGroup metricGroup) {
    }
  }

  @BeforeEach
  public void setUp() {
    SendErrorsTestReporter.reset();

    env = StreamExecutionEnvironment.getExecutionEnvironment(new Configuration() {
      {
        this.setString(ConfigConstants.METRICS_REPORTER_PREFIX + "sendErrorsTestReporter." +
                       ConfigConstants.METRICS_REPORTER_CLASS_SUFFIX, SendErrorsTestReporter.class.getName());
      }
    });

    wireMockServer = new WireMockServer(SERVER_PORT);
    wireMockServer.start();
  }

  @AfterEach
  public void tearDown() {
    wireMockServer.stop();
  }

  @Test
  public void testConnection() throws Exception {
    wireMockServer.stubFor(any(urlPathEqualTo("/myendpoint"))
                               .withHeader("Content-Type", equalTo("application/json"))
                               .willReturn(
                                   aResponse().withHeader("Content-Type", "application/json")
                                              .withStatus(200)
                                              .withBody("{}")));

    var source = env.fromCollection(messages);
    var httpSink = HttpSink.<String>builder()
                           .setEndpointUrl("http://localhost:" + SERVER_PORT + "/myendpoint")
                           .setElementConverter(
                               (s, _context) -> new HttpSinkRequestEntry(
                                   "POST", "application/json", s.getBytes(StandardCharsets.UTF_8)))
                           .setSinkHttpClientBuilder(JavaNetSinkHttpClient::new)
                           .build();
    source.sinkTo(httpSink);
    env.execute("Http Sink test connection");

    var responses = wireMockServer.getAllServeEvents();
    assertTrue(responses.stream().allMatch(response -> Objects.equals(response.getRequest().getUrl(), "/myendpoint")));
    assertTrue(responses.stream().allMatch(response -> response.getResponse().getStatus() == 200));

    var requestsAsMap = responses.stream().map(response -> {
      try {
        return new ObjectMapper().readValue(response.getRequest().getBody(), HashMap.class);
      } catch (IOException e) {
        return null;
      }
    }).collect(Collectors.toList());
    assertTrue(requestsAsMap.stream().allMatch(Objects::nonNull));

    var idsSet = new HashSet<>(messageIds);
    for (var request : requestsAsMap) {
      var el = (Integer) request.get("http-sink-id");
      assertTrue(idsSet.contains(el));
      idsSet.remove(el);
    }
    assertTrue(idsSet.isEmpty());
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
                               (s, _context) -> new HttpSinkRequestEntry(
                                   "POST", "application/json", s.getBytes(StandardCharsets.UTF_8)))
                           .setSinkHttpClientBuilder(JavaNetSinkHttpClient::new)
                           .build();
    source.sinkTo(httpSink);
    env.execute("Http Sink test failed connection");

    assertEquals(1, SendErrorsTestReporter.getCount());
//    TODO: reintroduce along with the retries
//    var postedRequests = wireMockServer.findAll(postRequestedFor(urlPathEqualTo("/myendpoint")));
//    assertEquals(2, postedRequests.size());
//    assertEquals(postedRequests.get(0).getBodyAsString(), postedRequests.get(1).getBodyAsString());
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
                               (s, _context) -> new HttpSinkRequestEntry(
                                   "POST", "application/json", s.getBytes(StandardCharsets.UTF_8)))
                           .setSinkHttpClientBuilder(JavaNetSinkHttpClient::new)
                           .build();
    source.sinkTo(httpSink);
    env.execute("Http Sink test failed connection");

    assertEquals(1, SendErrorsTestReporter.getCount());
//    var postedRequests = wireMockServer.findAll(postRequestedFor(urlPathEqualTo("/myendpoint")));
//    assertEquals(2, postedRequests.size());
//    assertEquals(postedRequests.get(0).getBodyAsString(), postedRequests.get(1).getBodyAsString());
  }
}
