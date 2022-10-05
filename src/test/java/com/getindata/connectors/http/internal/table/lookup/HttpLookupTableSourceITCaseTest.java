package com.getindata.connectors.http.internal.table.lookup;

import java.io.File;
import java.util.Comparator;
import java.util.Objects;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.client.MappingBuilder;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.stubbing.StubMapping;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.StringUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.matchingJsonPath;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.put;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;

@Slf4j
public class HttpLookupTableSourceITCaseTest {

    private static final int SERVER_PORT = 9090;

    private static final int HTTPS_SERVER_PORT = 8443;

    private static final String CERTS_PATH = "src/test/resources/security/certs/";

    private static final String SERVER_KEYSTORE_PATH =
        "src/test/resources/security/certs/serverKeyStore.jks";

    private static final String SERVER_TRUSTSTORE_PATH =
        "src/test/resources/security/certs/serverTrustStore.jks";

    private static final String ENDPOINT = "/client";

    /**
     * Comparator for Flink SQL result.
     */
    private final Comparator<Row> rowComparator = (row1, row2) -> {
        String row1Id = (String) Objects.requireNonNull(row1.getField("id"));
        String row2Id = (String) Objects.requireNonNull(row2.getField("id"));

        return row1Id.compareTo(row2Id);
    };

    private StreamTableEnvironment tEnv;

    private WireMockServer wireMockServer;

    @SuppressWarnings("unchecked")
    @BeforeEach
    public void setup() {

        File keyStoreFile = new File(SERVER_KEYSTORE_PATH);
        File trustStoreFile = new File(SERVER_TRUSTSTORE_PATH);

        wireMockServer = new WireMockServer(
            WireMockConfiguration.wireMockConfig()
                .port(SERVER_PORT)
                .httpsPort(HTTPS_SERVER_PORT)
                .keystorePath(keyStoreFile.getAbsolutePath())
                .keystorePassword("password")
                .keyManagerPassword("password")
                .needClientAuth(true)
                .trustStorePath(trustStoreFile.getAbsolutePath())
                .trustStorePassword("password")
                .extensions(JsonTransform.class)
        );
        wireMockServer.start();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRestartStrategy(RestartStrategies.noRestart());
        tEnv = StreamTableEnvironment.create(env);
    }

    @AfterEach
    public void tearDown() {
        wireMockServer.stop();
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "GET", "POST", "PUT"})
    public void testHttpLookupJoin(String methodName) throws Exception {

        if (StringUtils.isNullOrWhitespaceOnly(methodName) || methodName.equalsIgnoreCase("GET")) {
            setupServerStub(wireMockServer);
        } else {
            setUpServerBodyStub(methodName, wireMockServer);
        }

        String lookupTable =
            "CREATE TABLE Customers ("
                + "id STRING,"
                + "id2 STRING,"
                + "msg STRING,"
                + "uuid STRING,"
                + "details ROW<"
                + "isActive BOOLEAN,"
                + "nestedDetails ROW<"
                + "balance STRING"
                + ">"
                + ">"
                + ") WITH ("
                + "'format' = 'json',"
                + "'connector' = 'rest-lookup',"
                + ((StringUtils.isNullOrWhitespaceOnly(methodName)) ?
                    "" :
                    "'lookup-method' = '" + methodName + "',")
                + "'url' = 'http://localhost:9090/client',"
                + "'gid.connector.http.source.lookup.header.Content-Type' = 'application/json',"
                + "'asyncPolling' = 'true'"
                + ")";

        testLookupJoin(lookupTable);
    }

    @Test
    public void testHttpsMTlsLookupJoin() throws Exception {

        File serverTrustedCert = new File(CERTS_PATH + "ca.crt");
        File clientCert = new File(CERTS_PATH + "client.crt");
        File clientPrivateKey = new File(CERTS_PATH + "clientPrivateKey.pem");

        setupServerStub(wireMockServer);

        String lookupTable =
            String.format("CREATE TABLE Customers ("
                    + "id STRING,"
                    + "id2 STRING,"
                    + "msg STRING,"
                    + "uuid STRING,"
                    + "details ROW<"
                    + "isActive BOOLEAN,"
                    + "nestedDetails ROW<"
                    + "balance STRING"
                    + ">"
                    + ">"
                    + ") WITH ("
                    + "'format' = 'json',"
                    + "'connector' = 'rest-lookup',"
                    + "'url' = 'https://localhost:" + HTTPS_SERVER_PORT + "/client',"
                    + "'gid.connector.http.source.lookup.header.Content-Type' = 'application/json',"
                    + "'asyncPolling' = 'true',"
                    + "'gid.connector.http.security.cert.server' = '%s',"
                    + "'gid.connector.http.security.cert.client' = '%s',"
                    + "'gid.connector.http.security.key.client' = '%s'"
                    + ")",
                serverTrustedCert.getAbsolutePath(),
                clientCert.getAbsolutePath(),
                clientPrivateKey.getAbsolutePath()
            );

        testLookupJoin(lookupTable);
    }

    private void testLookupJoin(String lookupTable) throws Exception {

        String sourceTable =
            "CREATE TABLE Orders ("
                + "id STRING,"
                + " id2 STRING,"
                + " proc_time AS PROCTIME()"
                + ") WITH ("
                + "'connector' = 'datagen',"
                + "'rows-per-second' = '1',"
                + "'fields.id.kind' = 'sequence',"
                + "'fields.id.start' = '1',"
                + "'fields.id.end' = '5',"
                + "'fields.id2.kind' = 'sequence',"
                + "'fields.id2.start' = '2',"
                + "'fields.id2.end' = '5'"
                + ")";

        tEnv.executeSql(sourceTable);
        tEnv.executeSql(lookupTable);

        // SQL query that performs JOIN on both tables.
        String joinQuery =
            "SELECT o.id, o.id2, c.msg, c.uuid, c.isActive, c.balance FROM Orders AS o "
                + "JOIN Customers FOR SYSTEM_TIME AS OF o.proc_time AS c "
                + "ON o.id = c.id "
                + "AND o.id2 = c.id2";

        TableResult result = tEnv.executeSql(joinQuery);
        result.await(15, TimeUnit.SECONDS);

        // We want to have sort the result by "id" to make validation easier.
        SortedSet<Row> collectedRows = new TreeSet<>(rowComparator);
        try (CloseableIterator<Row> joinResult = result.collect()) {
            while (joinResult.hasNext()) {
                Row row = joinResult.next();
                log.info("Collected row " + row);
                collectedRows.add(row);
            }
        }

        // validate every row and its column.
        assertAll(() -> {
                assertThat(collectedRows.size()).isEqualTo(4);
                int intElement = 0;
                for (Row row : collectedRows) {
                    intElement++;
                    assertThat(row.getArity()).isEqualTo(6);

                    // "id" nad "id2" columns should be different for every row.
                    assertThat(row.getField("id")).isEqualTo(String.valueOf(intElement));
                    assertThat(row.getField("id2")).isEqualTo(String.valueOf(intElement + 1));

                    assertThat(row.getField("uuid"))
                        .isEqualTo("fbb68a46-80a9-46da-9d40-314b5287079c");
                    assertThat(row.getField("isActive")).isEqualTo(true);
                    assertThat(row.getField("balance")).isEqualTo("$1,729.34");
                }
            }
        );
    }

    private void setupServerStub(WireMockServer wireMockServer) {
        StubMapping stubMapping = wireMockServer.stubFor(
            get(urlPathEqualTo(ENDPOINT))
                .withHeader("Content-Type", equalTo("application/json"))
                .willReturn(
                    aResponse()
                        .withTransformers(JsonTransform.NAME)));

        wireMockServer.addStubMapping(stubMapping);
    }

    private void setUpServerBodyStub(String methodName, WireMockServer wireMockServer) {

        MappingBuilder methodStub = (methodName.equalsIgnoreCase("PUT") ?
            put(urlEqualTo(ENDPOINT)) :
            post(urlEqualTo(ENDPOINT)));

        StubMapping stubMapping = wireMockServer.stubFor(
            methodStub
                .withHeader("Content-Type", equalTo("application/json"))
                .withRequestBody(matchingJsonPath("$.id"))
                .withRequestBody(matchingJsonPath("$.id2"))
                .willReturn(
                    aResponse()
                        .withTransformers(JsonTransform.NAME)));

        wireMockServer.addStubMapping(stubMapping);
    }
}
