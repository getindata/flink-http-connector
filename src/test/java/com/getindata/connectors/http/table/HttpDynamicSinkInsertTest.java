package com.getindata.connectors.http.table;

import com.getindata.connectors.http.internal.table.sink.HttpDynamicTableSinkFactory;
import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.http.RequestMethod;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Set;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class HttpDynamicSinkInsertTest {
  private static final int SERVER_PORT = 9090;

  protected StreamExecutionEnvironment env;
  protected StreamTableEnvironment tEnv;

  private WireMockServer wireMockServer;

  @BeforeEach
  public void setup() {
    wireMockServer = new WireMockServer(SERVER_PORT);
    wireMockServer.start();

    env = StreamExecutionEnvironment.getExecutionEnvironment();
    tEnv = StreamTableEnvironment.create(env);
  }

  @AfterEach
  public void tearDown() {
    wireMockServer.stop();
  }

  @Test
  public void testHttpDynamicSinkDefaultPost() throws Exception {
    wireMockServer.stubFor(any(urlPathEqualTo("/myendpoint")).willReturn(ok()));

    final String createTable =
        String.format(
            "CREATE TABLE http (\n"
            + "  id bigint,\n"
            + "  first_name string,\n"
            + "  last_name string,\n"
            + "  gender string,\n"
            + "  stock string,\n"
            + "  currency string,\n"
            + "  tx_date timestamp(3)\n"
            + ") with (\n"
            + "  'connector' = '%s',\n"
            + "  'url' = '%s',\n"
            + "  'format' = 'json'\n"
            + ")",
            HttpDynamicTableSinkFactory.IDENTIFIER,
            "http://localhost:" + SERVER_PORT + "/myendpoint"
        );

    tEnv.executeSql(createTable);

    final String insert = "INSERT INTO http\n"
                          + "VALUES (1, 'Ninette', 'Clee', 'Female', 'CDZI', 'RUB', TIMESTAMP '2021-08-24 15:22:59')";
    tEnv.executeSql(insert).await();

    var postedRequests = wireMockServer.findAll(anyRequestedFor(urlPathEqualTo("/myendpoint")));
    assertEquals(1, postedRequests.size());

    var request = postedRequests.get(0);
    assertEquals(
        "{\"id\":1,\"first_name\":\"Ninette\",\"last_name\":\"Clee\",\"gender\":\"Female\",\"stock\":\"CDZI\",\"currency\":\"RUB\",\"tx_date\":\"2021-08-24 15:22:59\"}",
        request.getBodyAsString()
    );
    assertEquals(RequestMethod.POST, request.getMethod());
    assertEquals("application/json", request.getHeader("Content-Type"));
  }

  @Test
  public void testHttpDynamicSinkPut() throws Exception {
    wireMockServer.stubFor(any(urlPathEqualTo("/myendpoint")).willReturn(ok()));

    final String createTable =
        String.format(
            "CREATE TABLE http (\n"
            + "  id bigint,\n"
            + "  first_name string,\n"
            + "  last_name string,\n"
            + "  gender string,\n"
            + "  stock string,\n"
            + "  currency string,\n"
            + "  tx_date timestamp(3)\n"
            + ") with (\n"
            + "  'connector' = '%s',\n"
            + "  'url' = '%s',\n"
            + "  'insert-method' = 'PUT',\n"
            + "  'format' = 'json'\n"
            + ")",
            HttpDynamicTableSinkFactory.IDENTIFIER,
            "http://localhost:" + SERVER_PORT + "/myendpoint"
        );

    tEnv.executeSql(createTable);

    final String insert = "INSERT INTO http\n"
                          + "VALUES\n"
                          + " (1, 'Ninette', 'Clee', 'Female', 'CDZI', 'RUB', TIMESTAMP '2021-08-24 15:22:59'),\n"
                          + " (2, 'Hedy', 'Hedgecock', 'Female', 'DGICA', 'CNY', TIMESTAMP '2021-10-24 20:53:54')";
    tEnv.executeSql(insert).await();

    var postedRequests = wireMockServer.findAll(anyRequestedFor(urlPathEqualTo("/myendpoint")));
    assertEquals(2, postedRequests.size());

    var jsonRequests = new HashSet<>(Set.of(
        "{\"id\":1,\"first_name\":\"Ninette\",\"last_name\":\"Clee\",\"gender\":\"Female\",\"stock\":\"CDZI\",\"currency\":\"RUB\",\"tx_date\":\"2021-08-24 15:22:59\"}",
        "{\"id\":2,\"first_name\":\"Hedy\",\"last_name\":\"Hedgecock\",\"gender\":\"Female\",\"stock\":\"DGICA\",\"currency\":\"CNY\",\"tx_date\":\"2021-10-24 20:53:54\"}"
    ));
    for (var request : postedRequests) {
      assertEquals(RequestMethod.PUT, request.getMethod());
      assertEquals("application/json", request.getHeader("Content-Type"));
      assertTrue(jsonRequests.contains(request.getBodyAsString()));
      jsonRequests.remove(request.getBodyAsString());
    }
  }

  @Test
  public void testHttpDynamicSinkRawFormat() throws Exception {
    wireMockServer.stubFor(any(urlPathEqualTo("/myendpoint")).willReturn(ok()));

    final String createTable =
        String.format(
            "CREATE TABLE http (\n"
            + "  last_name string"
            + ") with (\n"
            + "  'connector' = '%s',\n"
            + "  'url' = '%s',\n"
            + "  'format' = 'raw'\n"
            + ")",
            HttpDynamicTableSinkFactory.IDENTIFIER,
            "http://localhost:" + SERVER_PORT + "/myendpoint"
        );

    tEnv.executeSql(createTable);

    final String insert = "INSERT INTO http VALUES ('Clee')";
    tEnv.executeSql(insert).await();

    var postedRequests = wireMockServer.findAll(anyRequestedFor(urlPathEqualTo("/myendpoint")));
    assertEquals(1, postedRequests.size());

    var request = postedRequests.get(0);
    assertEquals("Clee", request.getBodyAsString());
    assertEquals(RequestMethod.POST, request.getMethod());
    assertEquals("application/octet-stream", request.getHeader("Content-Type"));
  }
}
