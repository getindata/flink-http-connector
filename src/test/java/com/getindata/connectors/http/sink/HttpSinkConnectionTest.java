package com.getindata.connectors.http.sink;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.junit.WireMockRule;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static com.github.tomakehurst.wiremock.stubbing.Scenario.STARTED;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class HttpSinkConnectionTest {
  private static final int SERVER_PORT = 9090;
  private static final Set<Integer> messageIds = IntStream.range(0, 50).boxed().collect(Collectors.toSet());
  private static final List<String> messages =
      messageIds.stream().map(i -> "{\"http-sink-id\":" + i + "}").collect(Collectors.toList());

  @Rule
  public WireMockRule wireMockRule = new WireMockRule(WireMockConfiguration.wireMockConfig().port(SERVER_PORT));

  @Test
  public void testConnection() throws Exception {
    wireMockRule.stubFor(any(urlPathEqualTo("/myendpoint")).willReturn(
        aResponse().withHeader("Content-Type", "text/json").withStatus(200).withBody("{}")));

    var env = StreamExecutionEnvironment.getExecutionEnvironment();
    var source = env.fromCollection(messages);
    var httpSink = HttpSink.<String>builder()
                           .setEndpointUrl("http://localhost:" + SERVER_PORT + "/myendpoint")
                           .setElementConverter(
                               (s, _context) -> new HttpSinkRequestEntry("POST", "application/json", s.getBytes(StandardCharsets.UTF_8)))
                           .build();
    source.sinkTo(httpSink);
    env.execute("Http Sink test connection");

    var postedRequests = wireMockRule.findAll(postRequestedFor(urlPathEqualTo("/myendpoint")));
    var requestsAsMap = postedRequests.stream().map(pr -> {
      try {
        return new ObjectMapper().readValue(pr.getBody(), HashMap.class);
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
  public void testFailedConnection() throws Exception {
    wireMockRule.stubFor(any(urlPathEqualTo("/myendpoint"))
                             .inScenario("Retry Scenario")
                             .whenScenarioStateIs(STARTED)
                             .willReturn(serverError())
                             .willSetStateTo("Cause Success"));
    wireMockRule.stubFor(any(urlPathEqualTo("/myendpoint"))
                             .inScenario("Retry Scenario")
                             .whenScenarioStateIs("Cause Success")
                             .willReturn(aResponse().withStatus(200))
                             .willSetStateTo("Cause Success"));

    var env = StreamExecutionEnvironment.getExecutionEnvironment();
    var source = env.fromCollection(List.of(messages.get(0)));
    var httpSink = HttpSink.<String>builder()
                           .setEndpointUrl("http://localhost:" + SERVER_PORT + "/myendpoint")
                           .setElementConverter(
                               (s, _context) -> new HttpSinkRequestEntry("POST", "application/json", s.getBytes(StandardCharsets.UTF_8)))
                           .build();
    source.sinkTo(httpSink);
    env.execute("Http Sink test connection");

    var postedRequests = wireMockRule.findAll(postRequestedFor(urlPathEqualTo("/myendpoint")));
    assertEquals(2, postedRequests.size());
    assertEquals(postedRequests.get(0).getBodyAsString(), postedRequests.get(1).getBodyAsString());
  }
}
