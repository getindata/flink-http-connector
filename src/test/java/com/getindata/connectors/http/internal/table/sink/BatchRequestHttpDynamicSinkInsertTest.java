package com.getindata.connectors.http.internal.table.sink;

import java.io.File;
import java.util.Arrays;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.http.RequestMethod;
import com.github.tomakehurst.wiremock.verification.LoggedRequest;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.options;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

import static com.getindata.connectors.http.TestHelper.readTestFile;

public class BatchRequestHttpDynamicSinkInsertTest {

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

    private static Stream<Arguments> requestBatch() {
        return Stream.of(
            Arguments.of(50, "allInOneBatch.txt"),
            Arguments.of(5, "allInOneBatch.txt"),
            Arguments.of(3, "twoBatches.txt"),
            Arguments.of(2, "threeBatches.txt"),
            Arguments.of(1, "fourSingleEventBatches.txt")
        );
    }

    @ParameterizedTest
    @MethodSource("requestBatch")
    public void testHttpDynamicSinkDefaultPost(int requestBatchSize, String expectedRequests)
            throws Exception {

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
                    + "  'gid.connector.http.sink.request.batch.size' = '%s',\n"
                    + "  'gid.connector.http.sink.header.Content-Type' = '%s'\n"
                    + ")",
                HttpDynamicTableSinkFactory.IDENTIFIER,
                "http://localhost:" + SERVER_PORT + "/myendpoint",
                requestBatchSize,
                contentTypeHeaderValue
            );

        tEnv.executeSql(createTable);

        final String insert = "INSERT INTO http\n"
            + "VALUES\n"
            + " (1, 'Ninette', 'Clee', 'Female', 'CDZI', 'RUB', TIMESTAMP '2021-08-24 15:22:59'),\n"
            + " (2, 'Rob', 'Zombie', 'Male', 'DGICA', 'GBP', TIMESTAMP '2021-10-25 20:53:54'), \n"
            + " (3, 'Adam', 'Jones', 'Male', 'DGICA', 'PLN', TIMESTAMP '2021-10-26 20:53:54'), \n"
            + " (4, 'Danny', 'Carey', 'Male', 'DGICA', 'USD', TIMESTAMP '2021-10-27 20:53:54'), \n"
            + " (5, 'Bob', 'Dylan', 'Male', 'DGICA', 'USD', TIMESTAMP '2021-10-28 20:53:54')";
        tEnv.executeSql(insert).await();

        verifyRequests(expectedRequests);
    }

    @ParameterizedTest
    @MethodSource("requestBatch")
    public void testHttpDynamicSinkPut(int requestBatchSize, String expectedRequests)
            throws Exception {

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
                    + "  'gid.connector.http.sink.request.batch.size' = '%s',\n"
                    + "  'gid.connector.http.sink.header.Content-Type' = '%s'\n"
                    + ")",
                HttpDynamicTableSinkFactory.IDENTIFIER,
                "http://localhost:" + SERVER_PORT + "/myendpoint",
                requestBatchSize,
                contentTypeHeaderValue
            );

        tEnv.executeSql(createTable);

        final String insert = "INSERT INTO http\n"
            + "VALUES\n"
            + " (1, 'Ninette', 'Clee', 'Female', 'CDZI', 'RUB', TIMESTAMP '2021-08-24 15:22:59'),\n"
            + " (2, 'Rob', 'Zombie', 'Male', 'DGICA', 'GBP', TIMESTAMP '2021-10-25 20:53:54'), \n"
            + " (3, 'Adam', 'Jones', 'Male', 'DGICA', 'PLN', TIMESTAMP '2021-10-26 20:53:54'), \n"
            + " (4, 'Danny', 'Carey', 'Male', 'DGICA', 'USD', TIMESTAMP '2021-10-27 20:53:54'), \n"
            + " (5, 'Bob', 'Dylan', 'Male', 'DGICA', 'USD', TIMESTAMP '2021-10-28 20:53:54')";
        tEnv.executeSql(insert).await();

        verifyRequests(expectedRequests);
    }

    private void verifyRequests(String expectedResponse) {
        ObjectMapper mapper = new ObjectMapper();

        var postedRequests = wireMockServer.findAll(anyRequestedFor(urlPathEqualTo("/myendpoint")))
            .stream()
            .map(LoggedRequest::getBodyAsString)
            .map(content -> getJsonSipleString(mapper, content))
            .collect(Collectors.toList());

        var expectedResponses =
            Arrays.stream(readTestFile("/json/sink/" + expectedResponse).split("#-----#"))
                .map(content -> getJsonSipleString(mapper, content)).collect(Collectors.toList());

        // TODO this ideally should use containsExactlyElementsOf however Wiremock uses multiple
        //  threads to add events to its internal journal which can brea the order of
        //  received events. Probably use WireMock Scenarios feature can help here and allow to
        //  verify the order. Or maybe there is some other solution for that.
        assertThat(postedRequests).containsExactlyInAnyOrderElementsOf(expectedResponses);
    }

    private static String getJsonSipleString(ObjectMapper mapper, String content) {
        try {
            return mapper.readTree(content).toString();
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
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
                    + "  'gid.connector.http.sink.header.Content-Type' = '%s'\n"
                    + ")",
                HttpDynamicTableSinkFactory.IDENTIFIER,
                "http://localhost:" + SERVER_PORT + "/myendpoint",
                contentTypeHeaderValue
            );

        tEnv.executeSql(createTable);

        final String insert = "INSERT INTO http VALUES ('Clee'), ('John')";
        tEnv.executeSql(insert).await();

        var postedRequests = wireMockServer.findAll(anyRequestedFor(urlPathEqualTo("/myendpoint")));
        assertEquals(1, postedRequests.size());

        var request = postedRequests.get(0);
        assertEquals("[Clee,John]", request.getBodyAsString());
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

        final String insert = "INSERT INTO http VALUES ('Clee'), ('John')";
        tEnv.executeSql(insert).await();

        var postedRequests = wireMockServer.findAll(anyRequestedFor(urlPathEqualTo("/myendpoint")));
        assertEquals(1, postedRequests.size());

        var request = postedRequests.get(0);
        assertEquals("[Clee,John]", request.getBodyAsString());
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
            "[{\"id\":1,\"first_name\":\"Ninette\",\"last_name\":\"Clee\","
                + "\"gender\":\"Female\",\"stock\":\"CDZI\",\"currency\":\"RUB\","
                + "\"tx_date\":\"2021-08-24 15:22:59\"}]",
            request.getBodyAsString()
        );

    }
}
