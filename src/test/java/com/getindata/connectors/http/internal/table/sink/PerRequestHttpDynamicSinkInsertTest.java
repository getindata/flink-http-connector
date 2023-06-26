package com.getindata.connectors.http.internal.table.sink;

import java.io.File;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.http.RequestMethod;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.options;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class PerRequestHttpDynamicSinkInsertTest {

    private static final int SERVER_PORT = 9090;

    private static final int HTTPS_SERVER_PORT = 8443;

    private static final String CERTS_PATH = "src/test/resources/security/certs/";

    private static final String SERVER_KEYSTORE_PATH =
        "src/test/resources/security/certs/serverKeyStore.jks";

    private static final String SERVER_TRUSTSTORE_PATH =
        "src/test/resources/security/certs/serverTrustStore.jks";

    protected StreamExecutionEnvironment env;

    protected StreamTableEnvironment tEnv;

    private WireMockServer wireMockServer;

    @BeforeEach
    public void setup() {
        File keyStoreFile = new File(SERVER_KEYSTORE_PATH);
        File trustStoreFile = new File(SERVER_TRUSTSTORE_PATH);

        this.wireMockServer = new WireMockServer(options()
            .port(SERVER_PORT)
            .httpsPort(HTTPS_SERVER_PORT)
            .keystorePath(keyStoreFile.getAbsolutePath())
            .keystorePassword("password")
            .keyManagerPassword("password")
            .needClientAuth(true)
            .trustStorePath(trustStoreFile.getAbsolutePath())
            .trustStorePassword("password")
        );

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
        String contentTypeHeaderValue = "application/json";

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
                    + "  'format' = 'json',\n"
                    + "  'gid.connector.http.sink.writer.request.mode' = 'single',\n"
                    + "  'gid.connector.http.sink.header.Content-Type' = '%s'\n"
                    + ")",
                HttpDynamicTableSinkFactory.IDENTIFIER,
                "http://localhost:" + SERVER_PORT + "/myendpoint",
                contentTypeHeaderValue
            );

        tEnv.executeSql(createTable);

        final String insert = "INSERT INTO http\n"
            + "VALUES (1, 'Ninette', 'Clee', 'Female', 'CDZI', 'RUB', "
            + "TIMESTAMP '2021-08-24 15:22:59')";
        tEnv.executeSql(insert).await();

        var postedRequests =
            wireMockServer.findAll(anyRequestedFor(urlPathEqualTo("/myendpoint")));
        assertEquals(1, postedRequests.size());

        var request = postedRequests.get(0);
        assertEquals(
            "{\"id\":1,\"first_name\":\"Ninette\",\"last_name\":\"Clee\","
                + "\"gender\":\"Female\",\"stock\":\"CDZI\",\"currency\":\"RUB\","
                + "\"tx_date\":\"2021-08-24 15:22:59\"}",
            request.getBodyAsString()
        );
        assertEquals(RequestMethod.POST, request.getMethod());
        assertEquals(contentTypeHeaderValue, request.getHeader("Content-Type"));
    }

    @Test
    public void testHttpDynamicSinkPut() throws Exception {
        wireMockServer.stubFor(any(urlPathEqualTo("/myendpoint")).willReturn(ok()));
        String contentTypeHeaderValue = "application/json";

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
                    + "  'format' = 'json',\n"
                    + "  'gid.connector.http.sink.writer.request.mode' = 'single',\n"
                    + "  'gid.connector.http.sink.header.Content-Type' = '%s'\n"
                    + ")",
                HttpDynamicTableSinkFactory.IDENTIFIER,
                "http://localhost:" + SERVER_PORT + "/myendpoint",
                contentTypeHeaderValue
            );

        tEnv.executeSql(createTable);

        final String insert = "INSERT INTO http\n"
            + "VALUES\n"
            + " (1, 'Ninette', 'Clee', 'Female', 'CDZI', 'RUB', TIMESTAMP '2021-08-24 15:22:59'),\n"
            + " (2, 'Hedy', 'Hedgecock', 'Female', 'DGICA', 'CNY', "
            + "TIMESTAMP '2021-10-24 20:53:54')";
        tEnv.executeSql(insert).await();

        var postedRequests = wireMockServer.findAll(anyRequestedFor(urlPathEqualTo("/myendpoint")));
        assertEquals(2, postedRequests.size());

        var jsonRequests = new HashSet<>(Set.of(
            "{\"id\":1,\"first_name\":\"Ninette\",\"last_name\":\"Clee\",\"gender\":\"Female\","
                + "\"stock\":\"CDZI\",\"currency\":\"RUB\",\"tx_date\":\"2021-08-24 15:22:59\"}",
            "{\"id\":2,\"first_name\":\"Hedy\",\"last_name\":\"Hedgecock\",\"gender\":\"Female\","
                + "\"stock\":\"DGICA\",\"currency\":\"CNY\",\"tx_date\":\"2021-10-24 20:53:54\"}"
        ));
        for (var request : postedRequests) {
            assertEquals(RequestMethod.PUT, request.getMethod());
            assertEquals(contentTypeHeaderValue, request.getHeader("Content-Type"));
            assertTrue(jsonRequests.contains(request.getBodyAsString()));
            jsonRequests.remove(request.getBodyAsString());
        }
    }

    @Test
    public void testHttpDynamicSinkRawFormat() throws Exception {
        wireMockServer.stubFor(any(urlPathEqualTo("/myendpoint")).willReturn(ok()));
        String contentTypeHeaderValue = "application/octet-stream";

        final String createTable =
            String.format(
                "CREATE TABLE http (\n"
                    + "  last_name string"
                    + ") with (\n"
                    + "  'connector' = '%s',\n"
                    + "  'url' = '%s',\n"
                    + "  'format' = 'raw',\n"
                    + "  'gid.connector.http.sink.writer.request.mode' = 'single',\n"
                    + "  'gid.connector.http.sink.header.Content-Type' = '%s'\n"
                    + ")",
                HttpDynamicTableSinkFactory.IDENTIFIER,
                "http://localhost:" + SERVER_PORT + "/myendpoint",
                contentTypeHeaderValue
            );

        tEnv.executeSql(createTable);

        final String insert = "INSERT INTO http VALUES ('Clee')";
        tEnv.executeSql(insert).await();

        var postedRequests = wireMockServer.findAll(anyRequestedFor(urlPathEqualTo("/myendpoint")));
        assertEquals(1, postedRequests.size());

        var request = postedRequests.get(0);
        assertEquals("Clee", request.getBodyAsString());
        assertEquals(RequestMethod.POST, request.getMethod());
        assertEquals(contentTypeHeaderValue, request.getHeader("Content-Type"));
    }

    @Test
    public void testHttpRequestWithHeadersFromDdl()
        throws ExecutionException, InterruptedException {
        String originHeaderValue = "*";
        String xContentTypeOptionsHeaderValue = "nosniff";
        String contentTypeHeaderValue = "application/json";

        wireMockServer.stubFor(any(urlPathEqualTo("/myendpoint")).willReturn(ok()));

        final String createTable =
            String.format(
                "CREATE TABLE http (\n"
                    + "  last_name string"
                    + ") with (\n"
                    + "  'connector' = '%s',\n"
                    + "  'url' = '%s',\n"
                    + "  'format' = 'raw',\n"
                    + "  'gid.connector.http.sink.writer.request.mode' = 'single',\n"
                    + "  'gid.connector.http.sink.header.Origin' = '%s',\n"
                    + "  'gid.connector.http.sink.header.X-Content-Type-Options' = '%s',\n"
                    + "  'gid.connector.http.sink.header.Content-Type' = '%s'\n"
                    + ")",
                HttpDynamicTableSinkFactory.IDENTIFIER,
                "http://localhost:" + SERVER_PORT + "/myendpoint",
                originHeaderValue,
                xContentTypeOptionsHeaderValue,
                contentTypeHeaderValue
            );

        tEnv.executeSql(createTable);

        final String insert = "INSERT INTO http VALUES ('Clee')";
        tEnv.executeSql(insert).await();

        var postedRequests = wireMockServer.findAll(anyRequestedFor(urlPathEqualTo("/myendpoint")));
        assertEquals(1, postedRequests.size());

        var request = postedRequests.get(0);
        assertEquals("Clee", request.getBodyAsString());
        assertEquals(RequestMethod.POST, request.getMethod());
        assertEquals(contentTypeHeaderValue, request.getHeader("Content-Type"));
        assertEquals(originHeaderValue, request.getHeader("Origin"));
        assertEquals(xContentTypeOptionsHeaderValue, request.getHeader("X-Content-Type-Options"));
    }

    @Test
    public void testHttpsWithMTls() throws Exception {

        File serverTrustedCert = new File(CERTS_PATH + "ca.crt");

        File clientCert = new File(CERTS_PATH + "client.crt");
        File clientPrivateKey = new File(CERTS_PATH + "clientPrivateKey.pem");

        wireMockServer.stubFor(any(urlPathEqualTo("/myendpoint")).willReturn(ok()));
        String contentTypeHeaderValue = "application/json";

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
                    + "  'format' = 'json',\n"
                    + "  'gid.connector.http.sink.writer.request.mode' = 'single',\n"
                    + "  'gid.connector.http.sink.header.Content-Type' = '%s',\n"
                    + "  'gid.connector.http.security.cert.server' = '%s',\n"
                    + "  'gid.connector.http.security.cert.client' = '%s',\n"
                    + "  'gid.connector.http.security.key.client' = '%s'\n"
                    + ")",
                HttpDynamicTableSinkFactory.IDENTIFIER,
                "https://localhost:" + HTTPS_SERVER_PORT + "/myendpoint",
                contentTypeHeaderValue,
                serverTrustedCert.getAbsolutePath(),
                clientCert.getAbsolutePath(),
                clientPrivateKey.getAbsolutePath()
            );

        tEnv.executeSql(createTable);

        final String insert = "INSERT INTO http\n"
            + "VALUES (1, 'Ninette', 'Clee', 'Female', 'CDZI', 'RUB', "
            + "TIMESTAMP '2021-08-24 15:22:59')";
        tEnv.executeSql(insert).await();

        var postedRequests =
            wireMockServer.findAll(anyRequestedFor(urlPathEqualTo("/myendpoint")));
        assertEquals(1, postedRequests.size());

        var request = postedRequests.get(0);
        assertEquals(
            "{\"id\":1,\"first_name\":\"Ninette\",\"last_name\":\"Clee\","
                + "\"gender\":\"Female\",\"stock\":\"CDZI\",\"currency\":\"RUB\","
                + "\"tx_date\":\"2021-08-24 15:22:59\"}",
            request.getBodyAsString()
        );

    }
}
