package com.getindata.connectors.http.sink;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.getindata.connectors.http.sink.httpclient.JavaNetSinkHttpClient;
import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.http.Fault;
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

  private WireMockServer wireMockServer;

  @BeforeEach
  public void setUp() {
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

    var env = StreamExecutionEnvironment.getExecutionEnvironment();
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

    var env = StreamExecutionEnvironment.getExecutionEnvironment();
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

    var postedRequests = wireMockServer.findAll(postRequestedFor(urlPathEqualTo("/myendpoint")));
    assertEquals(2, postedRequests.size());
    assertEquals(postedRequests.get(0).getBodyAsString(), postedRequests.get(1).getBodyAsString());
  }

  @Test
  public void testFailedConnection() throws Exception {
    wireMockServer.stubFor(any(urlPathEqualTo("/myendpoint"))
                               .withHeader("Content-Type", equalTo("application/json"))
                               .inScenario("Retry Scenario")
                               .whenScenarioStateIs(STARTED)
                               .willReturn(serverError().withFault(Fault.EMPTY_RESPONSE))
                               .willSetStateTo("Cause Success"));
    wireMockServer.stubFor(any(urlPathEqualTo("/myendpoint"))
                               .withHeader("Content-Type", equalTo("application/json"))
                               .inScenario("Retry Scenario")
                               .whenScenarioStateIs("Cause Success")
                               .willReturn(aResponse().withStatus(200))
                               .willSetStateTo("Cause Success"));

    var env = StreamExecutionEnvironment.getExecutionEnvironment();
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

    var postedRequests = wireMockServer.findAll(postRequestedFor(urlPathEqualTo("/myendpoint")));
    assertEquals(2, postedRequests.size());
    assertEquals(postedRequests.get(0).getBodyAsString(), postedRequests.get(1).getBodyAsString());
  }
}
