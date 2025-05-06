package com.getindata.connectors.http.internal.table.lookup;

import java.io.File;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.client.MappingBuilder;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.matching.StringValuePattern;
import com.github.tomakehurst.wiremock.stubbing.Scenario;
import com.github.tomakehurst.wiremock.stubbing.StubMapping;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.connector.source.lookup.cache.LookupCache;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.binary.BinaryStringData;
import org.apache.flink.table.runtime.functions.table.lookup.LookupCacheManager;
import org.apache.flink.table.test.lookup.cache.LookupCacheAssert;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.StringUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

@Slf4j
class HttpLookupTableSourceITCaseTest {

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
    private static final Comparator<Row> ROW_COMPARATOR = (row1, row2) -> {
        String row1Id = (String) Objects.requireNonNull(row1.getField("id"));
        String row2Id = (String) Objects.requireNonNull(row2.getField("id"));

        return row1Id.compareTo(row2Id);
    };

    private StreamTableEnvironment tEnv;

    private WireMockServer wireMockServer;

    @SuppressWarnings("unchecked")
    @BeforeEach
    void setup() {

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
        Configuration config = new Configuration();
        config.set(ExecutionOptions.RUNTIME_MODE, RuntimeExecutionMode.STREAMING);
        env.configure(config, getClass().getClassLoader());
        env.enableCheckpointing(1000, CheckpointingMode.EXACTLY_ONCE);
        env.setParallelism(1);  // wire mock server has problem with scenario state during parallel execution

        tEnv = StreamTableEnvironment.create(env);
    }

    @AfterEach
    void tearDown() {
        wireMockServer.stop();
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "GET", "POST", "PUT"})
    void testHttpLookupJoin(String methodName) throws Exception {

        // GIVEN
        if (StringUtils.isNullOrWhitespaceOnly(methodName) || methodName.equalsIgnoreCase("GET")) {
            setupServerStub(wireMockServer);
        } else {
            setUpServerBodyStub(
                methodName,
                wireMockServer,
                List.of(matchingJsonPath("$.id"), matchingJsonPath("$.id2"))
            );
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
                + "'asyncPolling' = 'true',"
                + "'table.exec.async-lookup.buffer-capacity' = '50',"
                + "'table.exec.async-lookup.timeout' = '120s'"
                + ")";

        // WHEN
        SortedSet<Row> rows = testLookupJoin(lookupTable, 4);

        // THEN
        assertEnrichedRows(rows);
    }

    @Test
    void testHttpLookupJoinNoDataFromEndpoint() {

        // GIVEN
        setupServerStubEmptyResponse(wireMockServer);

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
                + "'url' = 'http://localhost:9090/client',"
                + "'gid.connector.http.source.lookup.header.Content-Type' = 'application/json',"
                + "'asyncPolling' = 'true'"
                + ")";

        // WHEN/THEN
        assertThrows(TimeoutException.class, () -> testLookupJoin(lookupTable, 4));
    }

    @Test
    void testLookupWithRetry() throws Exception {
        wireMockServer.stubFor(get(urlPathEqualTo(ENDPOINT))
                .inScenario("retry")
                .whenScenarioStateIs(Scenario.STARTED)
                .withHeader("Content-Type", equalTo("application/json"))
                .withQueryParam("id", matching("[0-9]+"))
                .withQueryParam("id2", matching("[0-9]+"))
                .willReturn(aResponse().withBody(new byte[0]).withStatus(501))
                .willSetStateTo("temporal_issue_gone")
        );
        wireMockServer.stubFor(get(urlPathEqualTo(ENDPOINT))
                .inScenario("retry")
                .whenScenarioStateIs("temporal_issue_gone")
                .withHeader("Content-Type", equalTo("application/json"))
                .withQueryParam("id", matching("[0-9]+"))
                .withQueryParam("id2", matching("[0-9]+"))
                .willReturn(aResponse().withTransformers(JsonTransform.NAME).withStatus(200))
        );

        var lookupTable =
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
                        + "'url' = 'http://localhost:9090/client',"
                        + "'lookup.max-retries' = '3',"
                        + "'gid.connector.http.source.lookup.header.Content-Type' = 'application/json',"
                        + "'gid.connector.http.source.lookup.retry-strategy.type' = 'fixed-delay',"
                        + "'gid.connector.http.source.lookup.retry-strategy.fixed-delay.delay' = '1ms',"
                        + "'gid.connector.http.source.lookup.success-codes' = '2XX',"
                        + "'gid.connector.http.source.lookup.retry-codes' = '501'"
                        + ")";

        var result = testLookupJoin(lookupTable, 1);

        assertEquals(1, result.size());
        wireMockServer.verify(2, getRequestedFor(urlPathEqualTo(ENDPOINT)));
    }

    @Test
    void testLookupIgnoreResponse() throws Exception {
        wireMockServer.stubFor(get(urlPathEqualTo(ENDPOINT))
                .inScenario("404_on_first")
                .whenScenarioStateIs(Scenario.STARTED)
                .withHeader("Content-Type", equalTo("application/json"))
                .withQueryParam("id", matching("[0-9]+"))
                .withQueryParam("id2", matching("[0-9]+"))
                .willReturn(aResponse().withBody(JsonTransform.NAME).withStatus(404))
                .willSetStateTo("second_request")
        );
        wireMockServer.stubFor(get(urlPathEqualTo(ENDPOINT))
                .inScenario("404_on_first")
                .whenScenarioStateIs("second_request")
                .withHeader("Content-Type", equalTo("application/json"))
                .withQueryParam("id", matching("[0-9]+"))
                .withQueryParam("id2", matching("[0-9]+"))
                .willReturn(aResponse().withTransformers(JsonTransform.NAME).withStatus(200))
        );

        var lookupTable =
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
                        + "'url' = 'http://localhost:9090/client',"
                        + "'gid.connector.http.source.lookup.header.Content-Type' = 'application/json',"
                        + "'gid.connector.http.source.lookup.success-codes' = '2XX,404',"
                        + "'gid.connector.http.source.lookup.ignored-response-codes' = '404'"
                        + ")";

        var result = testLookupJoin(lookupTable, 3);

        assertEquals(2, result.size());
        wireMockServer.verify(3, getRequestedFor(urlPathEqualTo(ENDPOINT)));
    }

    @Test
    void testHttpsMTlsLookupJoin() throws Exception {

        // GIVEN
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

        // WHEN
        SortedSet<Row> rows = testLookupJoin(lookupTable, 4);

        // THEN
        assertEnrichedRows(rows);
    }

    @Test
    void testLookupJoinProjectionPushDown() throws Exception {

        // GIVEN
        setUpServerBodyStub(
                "POST",
                wireMockServer,
                List.of(
                        matchingJsonPath("$.row.aStringColumn"),
                        matchingJsonPath("$.row.anIntColumn"),
                        matchingJsonPath("$.row.aFloatColumn")
                )
        );

        String fields =
                "`row` ROW<`aStringColumn` STRING, `anIntColumn` INT, `aFloatColumn` FLOAT>\n";

        String sourceTable =
                "CREATE TABLE Orders (\n"
                        + "  proc_time AS PROCTIME(),\n"
                        + "  id STRING,\n"
                        + fields
                        + ") WITH ("
                        + "'connector' = 'datagen',"
                        + "'rows-per-second' = '1',"
                        + "'fields.id.kind' = 'sequence',"
                        + "'fields.id.start' = '1',"
                        + "'fields.id.end' = '5'"
                        + ")";

        String lookupTable =
            "CREATE TABLE Customers (\n" +
                    "  `enrichedInt` INT,\n" +
                    "  `enrichedString` STRING,\n" +
                    "  \n"
                    + fields
                    + ") WITH ("
                    + "'format' = 'json',"
                    + "'lookup-request.format' = 'json',"
                    + "'lookup-request.format.json.fail-on-missing-field' = 'true',"
                    + "'connector' = 'rest-lookup',"
                    + "'lookup-method' = 'POST',"
                    + "'url' = 'http://localhost:9090/client',"
                    + "'gid.connector.http.source.lookup.header.Content-Type' = 'application/json',"
                    + "'asyncPolling' = 'true'"
                    + ")";

        tEnv.executeSql(sourceTable);
        tEnv.executeSql(lookupTable);

        // WHEN
        // SQL query that performs JOIN on both tables.
        String joinQuery =
                "CREATE TEMPORARY VIEW lookupResult AS " +
                        "SELECT o.id, o.`row`, c.enrichedInt, c.enrichedString FROM Orders AS o"
                        + " JOIN Customers FOR SYSTEM_TIME AS OF o.proc_time AS c"
                        + " ON (\n"
                        + "  o.`row` = c.`row`\n"
                        + ")";

        tEnv.executeSql(joinQuery);

        // SQL query that performs a projection pushdown to limit the number of columns
        String lastQuery =
                "SELECT r.id, r.enrichedInt FROM lookupResult r;";

        TableResult result = tEnv.executeSql(lastQuery);
        result.await(15, TimeUnit.SECONDS);

        // THEN
        SortedSet<Row> collectedRows = getCollectedRows(result);

        collectedRows.stream().forEach(row -> assertThat(row.getArity()).isEqualTo(2));

        assertThat(collectedRows.size()).isEqualTo(5);
    }

    @Test
    void testLookupJoinProjectionPushDownNested() throws Exception {

        // GIVEN
        setUpServerBodyStub(
                "POST",
                wireMockServer,
                List.of(
                        matchingJsonPath("$.row.aStringColumn"),
                        matchingJsonPath("$.row.anIntColumn"),
                        matchingJsonPath("$.row.aFloatColumn")
                )
        );

        String fields =
            "`row` ROW<`aStringColumn` STRING, `anIntColumn` INT, `aFloatColumn` FLOAT>\n";

        String sourceTable =
            "CREATE TABLE Orders (\n"
                    + "  proc_time AS PROCTIME(),\n"
                    + "  id STRING,\n"
                    + fields
                    + ") WITH ("
                    + "'connector' = 'datagen',"
                    + "'rows-per-second' = '1',"
                    + "'fields.id.kind' = 'sequence',"
                    + "'fields.id.start' = '1',"
                    + "'fields.id.end' = '5'"
                    + ")";

        String lookupTable =
            "CREATE TABLE Customers (\n" +
                    "  `enrichedInt` INT,\n" +
                    "  `enrichedString` STRING,\n" +
                    "  \n"
                    + fields
                    + ") WITH ("
                    + "'format' = 'json',"
                    + "'lookup-request.format' = 'json',"
                    + "'lookup-request.format.json.fail-on-missing-field' = 'true',"
                    + "'connector' = 'rest-lookup',"
                    + "'lookup-method' = 'POST',"
                    + "'url' = 'http://localhost:9090/client',"
                    + "'gid.connector.http.source.lookup.header.Content-Type' = 'application/json',"
                    + "'asyncPolling' = 'true'"
                    + ")";

        tEnv.executeSql(sourceTable);
        tEnv.executeSql(lookupTable);

        // WHEN
        // SQL query that performs JOIN on both tables.
        String joinQuery =
            "CREATE TEMPORARY VIEW lookupResult AS " +
                    "SELECT o.id, o.`row`, c.enrichedInt, c.enrichedString FROM Orders AS o"
                    + " JOIN Customers FOR SYSTEM_TIME AS OF o.proc_time AS c"
                    + " ON (\n"
                    + "  o.`row` = c.`row`\n"
                    + ")";

        tEnv.executeSql(joinQuery);

        // SQL query that performs a project pushdown to take a subset of columns with nested value
        String lastQuery =
            "SELECT r.id, r.enrichedInt, r.`row`.aStringColumn FROM lookupResult r;";

        TableResult result = tEnv.executeSql(lastQuery);
        result.await(15, TimeUnit.SECONDS);

        // THEN
        SortedSet<Row> collectedRows = getCollectedRows(result);

        collectedRows.stream().forEach(row -> assertThat(row.getArity()).isEqualTo(3));

        assertThat(collectedRows.size()).isEqualTo(5);
    }

    @Test
    void testLookupJoinOnRowType() throws Exception {

        // GIVEN
        setUpServerBodyStub(
            "POST",
            wireMockServer,
            List.of(
                matchingJsonPath("$.row.aStringColumn"),
                matchingJsonPath("$.row.anIntColumn"),
                matchingJsonPath("$.row.aFloatColumn")
            )
        );

        String fields =
            "`row` ROW<`aStringColumn` STRING, `anIntColumn` INT, `aFloatColumn` FLOAT>\n";

        String sourceTable =
            "CREATE TABLE Orders (\n"
                + "  proc_time AS PROCTIME(),\n"
                + "  id STRING,\n"
                + fields
                + ") WITH ("
                + "'connector' = 'datagen',"
                + "'rows-per-second' = '1',"
                + "'fields.id.kind' = 'sequence',"
                + "'fields.id.start' = '1',"
                + "'fields.id.end' = '5'"
                + ")";

        String lookupTable =
            "CREATE TABLE Customers (\n" +
                "  `enrichedInt` INT,\n" +
                "  `enrichedString` STRING,\n" +
                "  \n"
                + fields
                + ") WITH ("
                + "'format' = 'json',"
                + "'lookup-request.format' = 'json',"
                + "'lookup-request.format.json.fail-on-missing-field' = 'true',"
                + "'connector' = 'rest-lookup',"
                + "'lookup-method' = 'POST',"
                + "'url' = 'http://localhost:9090/client',"
                + "'gid.connector.http.source.lookup.header.Content-Type' = 'application/json',"
                + "'asyncPolling' = 'true'"
                + ")";

        tEnv.executeSql(sourceTable);
        tEnv.executeSql(lookupTable);

        // WHEN
        // SQL query that performs JOIN on both tables.
        String joinQuery =
            "SELECT o.id, o.`row`, c.enrichedInt, c.enrichedString FROM Orders AS o"
                + " JOIN Customers FOR SYSTEM_TIME AS OF o.proc_time AS c"
                + " ON (\n"
                + "  o.`row` = c.`row`\n"
                + ")";

        TableResult result = tEnv.executeSql(joinQuery);
        result.await(15, TimeUnit.SECONDS);

        // THEN
        SortedSet<Row> collectedRows = getCollectedRows(result);

        // TODO add assert on values
        assertThat(collectedRows.size()).isEqualTo(5);
    }

    @Test
    void testLookupJoinOnRowTypeAndRootColumn() throws Exception {

        // GIVEN
        setUpServerBodyStub(
            "POST",
            wireMockServer,
            List.of(
                matchingJsonPath("$.enrichedString"),
                matchingJsonPath("$.row.aStringColumn"),
                matchingJsonPath("$.row.anIntColumn"),
                matchingJsonPath("$.row.aFloatColumn")
            )
        );

        String fields =
            "`row` ROW<`aStringColumn` STRING, `anIntColumn` INT, `aFloatColumn` FLOAT>\n";

        String sourceTable =
            "CREATE TABLE Orders (\n"
                + "  proc_time AS PROCTIME(),\n"
                + "  id STRING,\n"
                + fields
                + ") WITH ("
                + "'connector' = 'datagen',"
                + "'rows-per-second' = '1',"
                + "'fields.id.kind' = 'sequence',"
                + "'fields.id.start' = '1',"
                + "'fields.id.end' = '5'"
                + ")";

        String lookupTable =
            "CREATE TABLE Customers (\n" +
                "  `enrichedInt` INT,\n" +
                "  `enrichedString` STRING,\n" +
                "  \n"
                + fields
                + ") WITH ("
                + "'format' = 'json',"
                + "'lookup-request.format' = 'json',"
                + "'lookup-request.format.json.fail-on-missing-field' = 'true',"
                + "'connector' = 'rest-lookup',"
                + "'lookup-method' = 'POST',"
                + "'url' = 'http://localhost:9090/client',"
                + "'gid.connector.http.source.lookup.header.Content-Type' = 'application/json',"
                + "'asyncPolling' = 'true'"
                + ")";

        tEnv.executeSql(sourceTable);
        tEnv.executeSql(lookupTable);

        // WHEN
        // SQL query that performs JOIN on both tables.
        String joinQuery =
            "SELECT o.id, o.`row`, c.enrichedInt, c.enrichedString FROM Orders AS o"
                + " JOIN Customers FOR SYSTEM_TIME AS OF o.proc_time AS c"
                + " ON (\n"
                + "  o.id = c.enrichedString AND\n"
                + "  o.`row` = c.`row`\n"
                + ")";

        TableResult result = tEnv.executeSql(joinQuery);
        result.await(15, TimeUnit.SECONDS);

        // THEN
        SortedSet<Row> collectedRows = getCollectedRows(result);

        // TODO add assert on values
        assertThat(collectedRows.size()).isEqualTo(5);
    }

    @Test
    void testLookupJoinOnRowWithRowType() throws Exception {
        testLookupJoinOnRowWithRowTypeImpl();
    }

    @ParameterizedTest
    @CsvSource({
        "user:password, Basic dXNlcjpwYXNzd29yZA==, false",
        "Basic dXNlcjpwYXNzd29yZA==, Basic dXNlcjpwYXNzd29yZA==, false",
        "abc123, abc123, true",
        "Basic dXNlcjpwYXNzd29yZA==, Basic dXNlcjpwYXNzd29yZA==, true",
        "Bearer dXNlcjpwYXNzd29yZA==, Bearer dXNlcjpwYXNzd29yZA==, true"
    })
    void testLookupWithUseRawAuthHeader(
            String authHeaderRawValue,
            String expectedAuthHeaderValue,
            boolean useRawAuthHeader) throws Exception {

        // Test with gid.connector.http.source.lookup.use-raw-authorization-header set to either
        // true or false, and asserting Authorization header is processed as expected, either with
        // transformation for Basic Auth, or kept as-is when it is not used for Basic Auth.
        testLookupJoinOnRowWithRowTypeImpl(
            authHeaderRawValue, expectedAuthHeaderValue, useRawAuthHeader);
    }

    private void testLookupJoinOnRowWithRowTypeImpl() throws Exception {
        testLookupJoinOnRowWithRowTypeImpl(null, null, false);
    }

    private void testLookupJoinOnRowWithRowTypeImpl(
            String authHeaderRawValue,
            String expectedAuthHeaderValue,
            boolean useRawAuthHeader) throws Exception {

        // GIVEN
        setUpServerBodyStub(
            "POST",
            wireMockServer,
            List.of(
                matchingJsonPath("$.nestedRow.aStringColumn"),
                matchingJsonPath("$.nestedRow.anIntColumn"),
                matchingJsonPath("$.nestedRow.aRow.anotherStringColumn"),
                matchingJsonPath("$.nestedRow.aRow.anotherIntColumn")
            ),
            // For testing the gid.connector.http.source.lookup.use-raw-authorization-header
            // configuration parameter:
            expectedAuthHeaderValue != null ? "Authorization" : null,
            expectedAuthHeaderValue // expected value of extra header
        );

        String fields =
            "  `nestedRow` ROW<" +
            "    `aStringColumn` STRING," +
            "    `anIntColumn` INT," +
            "    `aRow` ROW<`anotherStringColumn` STRING, `anotherIntColumn` INT>" +
            "   >\n";

        String sourceTable =
            "CREATE TABLE Orders (\n"
                + "  proc_time AS PROCTIME(),\n"
                + "  id STRING,\n"
                + fields
                + ") WITH ("
                + "'connector' = 'datagen',"
                + "'rows-per-second' = '1',"
                + "'fields.id.kind' = 'sequence',"
                + "'fields.id.start' = '1',"
                + "'fields.id.end' = '5'"
                + ")";

        String useRawAuthHeaderString = useRawAuthHeader ? "'true'" : "'false'";

        String lookupTable =
            "CREATE TABLE Customers (\n" +
                "  `enrichedInt` INT,\n" +
                "  `enrichedString` STRING,\n" +
                "  \n"
                + fields
                + ") WITH ("
                + "'format' = 'json',"
                + "'connector' = 'rest-lookup',"
                + "'lookup-method' = 'POST',"
                + "'url' = 'http://localhost:9090/client',"
                + "'gid.connector.http.source.lookup.header.Content-Type' = 'application/json',"
                + (authHeaderRawValue != null ?
                      ("'gid.connector.http.source.lookup.use-raw-authorization-header' = "
                          + useRawAuthHeaderString + ","
                          + "'gid.connector.http.source.lookup.header.Authorization' = '"
                          + authHeaderRawValue + "',")
                      : "")
                + "'asyncPolling' = 'true'"
                + ")";

        tEnv.executeSql(sourceTable);
        tEnv.executeSql(lookupTable);

        // SQL query that performs JOIN on both tables.
        String joinQuery =
            "SELECT o.id, o.`nestedRow`, c.enrichedInt, c.enrichedString FROM Orders AS o"
                + " JOIN Customers FOR SYSTEM_TIME AS OF o.proc_time AS c"
                + " ON (\n"
                + "  o.`nestedRow` = c.`nestedRow`\n"
                + ")";

        TableResult result = tEnv.executeSql(joinQuery);
        result.await(15, TimeUnit.SECONDS);

        // THEN
        SortedSet<Row> collectedRows = getCollectedRows(result);

        // TODO add assert on values
        assertThat(collectedRows.size()).isEqualTo(5);
    }

    @Test
    void testNestedLookupJoinWithoutCast() throws Exception {

        // TODO ADD MORE ASSERTS
        // GIVEN
        setUpServerBodyStub(
            "POST",
            wireMockServer,
            List.of(
                matchingJsonPath("$.bool"),
                matchingJsonPath("$.tinyint"),
                matchingJsonPath("$.smallint"),
                matchingJsonPath("$.map"),
                matchingJsonPath("$.doubles"),
                matchingJsonPath("$.multiSet"),
                matchingJsonPath("$.time"),
                matchingJsonPath("$.map2map")
            )
        );

        String fields =
            "  `bool` BOOLEAN,\n" +
                "  `tinyint` TINYINT,\n" +
                "  `smallint` SMALLINT,\n" +
                "  `idInt` INT,\n" +
                "  `bigint` BIGINT,\n" +
                "  `float` FLOAT,\n" +
                "  `name` STRING,\n" +
                "  `decimal` DECIMAL(9, 6),\n" +
                "  `doubles` ARRAY<DOUBLE>,\n" +
                "  `date` DATE,\n" +
                "  `time` TIME(0),\n" +
                "  `timestamp3` TIMESTAMP(3),\n" +
                "  `timestamp9` TIMESTAMP(9),\n" +
                "  `timestampWithLocalZone` TIMESTAMP_LTZ(9),\n" +
                "  `map` MAP<STRING, BIGINT>,\n" +
                "  `multiSet` MULTISET<STRING>,\n" +
                "  `map2map` MAP<STRING, MAP<STRING, INT>>,\n" +
                "  `row` ROW<`aStringColumn` STRING, `anIntColumn` INT, `aFloatColumn` FLOAT>,\n" +
                "  `nestedRow` ROW<" +
                "    `aStringColumn` STRING," +
                "    `anIntColumn` INT," +
                "    `aRow` ROW<`anotherStringColumn` STRING, `anotherIntColumn` INT>" +
                "   >,\n" +
                "  `aTable` ARRAY<ROW<" +
                "      `aStringColumn` STRING," +
                "      `anIntColumn` INT," +
                "      `aFloatColumn` FLOAT" +
                "  >>\n";

        String sourceTable =
            "CREATE TABLE Orders (\n"
                + "id STRING,"
                + "  proc_time AS PROCTIME(),\n"
                + fields
                + ") WITH ("
                + "'connector' = 'datagen',"
                + "'rows-per-second' = '1',"
                + "'fields.id.kind' = 'sequence',"
                + "'fields.id.start' = '1',"
                + "'fields.id.end' = '5'"
                + ")";

        String lookupTable =
            "CREATE TABLE Customers (\n" +
                "  `enrichedInt` INT,\n" +
                "  `enrichedString` STRING,\n" +
                "  \n"
                + fields
                + ") WITH ("
                + "'format' = 'json',"
                + "'lookup-request.format' = 'json',"
                + "'lookup-request.format.json.fail-on-missing-field' = 'true',"
                + "'lookup-method' = 'POST',"
                + "'connector' = 'rest-lookup',"
                + "'url' = 'http://localhost:9090/client',"
                + "'gid.connector.http.source.lookup.header.Content-Type' = 'application/json',"
                + "'asyncPolling' = 'true'"
                + ")";

        tEnv.executeSql(sourceTable);
        tEnv.executeSql(lookupTable);

        // SQL query that performs JOIN on both tables.
        String joinQuery =
            "SELECT o.id, o.name, c.enrichedInt, c.enrichedString FROM Orders AS o"
                + " JOIN Customers FOR SYSTEM_TIME AS OF o.proc_time AS c"
                + " ON (\n"
                + "  o.`bool` = c.`bool` AND\n"
                + "  o.`tinyint` = c.`tinyint` AND\n"
                + "  o.`smallint` = c.`smallint` AND\n"
                + "  o.idInt = c.idInt AND\n"
                + "  o.`bigint` = c.`bigint` AND\n"
                + "  o.`float` = c.`float` AND\n"
                + "  o.name = c.name AND\n"
                + "  o.`decimal` = c.`decimal` AND\n"
                + "  o.doubles = c.doubles AND\n"
                + "  o.`date` = c.`date` AND\n"
                + "  o.`time` = c.`time` AND\n"
                + "  o.timestamp3 = c.timestamp3 AND\n"
                + "  o.timestamp9 = c.timestamp9 AND\n"
                + "  o.timestampWithLocalZone = c.timestampWithLocalZone AND\n"
                + "  o.`map` = c.`map` AND\n"
                + "  o.`multiSet` = c.`multiSet` AND\n"
                + "  o.map2map = c.map2map AND\n"
                + "  o.`row` = c.`row` AND\n"
                + "  o.nestedRow = c.nestedRow AND\n"
                + "  o.aTable = c.aTable\n"
                + ")";

        TableResult result = tEnv.executeSql(joinQuery);
        result.await(15, TimeUnit.SECONDS);

        // THEN
        SortedSet<Row> collectedRows = getCollectedRows(result);

        // TODO add assert on values
        assertThat(collectedRows.size()).isEqualTo(5);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testHttpLookupJoinWithCache(boolean isAsync) throws Exception {
        // GIVEN
        LookupCacheManager.keepCacheOnRelease(true);

        setupServerStub(wireMockServer);

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
                + "'lookup-method' = 'GET',"
                + "'url' = 'http://localhost:9090/client',"
                + "'gid.connector.http.source.lookup.header.Content-Type' = 'application/json',"
                + (isAsync ? "'asyncPolling' = 'true'," : "")
                + "'lookup.cache' = 'partial',"
                + "'lookup.partial-cache.max-rows' = '100'"
                + ")";

        // WHEN
        SortedSet<Row> rows = testLookupJoin(lookupTable, 4);

        // THEN
        try {
            assertEnrichedRows(rows);

            LookupCacheAssert.assertThat(getCache()).hasSize(4)
                .containsKey(GenericRowData.of(
                    BinaryStringData.fromString("3"), BinaryStringData.fromString("4")))
                .containsKey(GenericRowData.of(
                    BinaryStringData.fromString("4"), BinaryStringData.fromString("5")))
                .containsKey(GenericRowData.of(
                    BinaryStringData.fromString("1"), BinaryStringData.fromString("2")))
                .containsKey(GenericRowData.of(
                    BinaryStringData.fromString("2"), BinaryStringData.fromString("3")));
        } finally {
            LookupCacheManager.getInstance().checkAllReleased();
            LookupCacheManager.getInstance().clear();
            LookupCacheManager.keepCacheOnRelease(false);
        }
    }

    private LookupCache getCache() {
        Map<String, LookupCacheManager.RefCountedCache> managedCaches =
                LookupCacheManager.getInstance().getManagedCaches();
        assertThat(managedCaches).as("There should be only 1 shared cache registered").hasSize(1);
        return managedCaches.get(managedCaches.keySet().iterator().next()).getCache();
    }

    private @NotNull SortedSet<Row> testLookupJoin(String lookupTable, int maxRows) throws Exception {

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
                + "'fields.id.end' = '" + maxRows + "',"
                + "'fields.id2.kind' = 'sequence',"
                + "'fields.id2.start' = '2',"
                + "'fields.id2.end' = '" + (maxRows + 1) + "'"
                + ")";

        tEnv.executeSql(sourceTable);
        tEnv.executeSql(lookupTable);

        // WHEN
        // SQL query that performs JOIN on both tables.
        String joinQuery =
            "SELECT o.id, o.id2, c.msg, c.uuid, c.isActive, c.balance FROM Orders AS o "
                + "JOIN Customers FOR SYSTEM_TIME AS OF o.proc_time AS c "
                + "ON o.id = c.id "
                + "AND o.id2 = c.id2";

        TableResult result = tEnv.executeSql(joinQuery);
        result.await(15, TimeUnit.SECONDS);

        // THEN
        return getCollectedRows(result);
    }

    private void assertEnrichedRows(Collection<Row> collectedRows) {
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

    @NotNull
    private SortedSet<Row> getCollectedRows(TableResult result) throws Exception {

        // We want to sort the result by "id" to make validation easier.
        SortedSet<Row> collectedRows = new TreeSet<>(ROW_COMPARATOR);
        try (CloseableIterator<Row> joinResult = result.collect()) {
            while (joinResult.hasNext()) {
                Row row = joinResult.next();
                log.info("Collected row " + row);
                collectedRows.add(row);
            }
        }
        return collectedRows;
    }

    private void setupServerStub(WireMockServer wireMockServer) {
        StubMapping stubMapping = wireMockServer.stubFor(
            get(urlPathEqualTo(ENDPOINT))
                .withHeader("Content-Type", equalTo("application/json"))
                .withQueryParam("id", matching("[0-9]+"))
                .withQueryParam("id2", matching("[0-9]+"))
                .willReturn(
                    aResponse()
                        .withTransformers(JsonTransform.NAME)
                )
        );

        wireMockServer.addStubMapping(stubMapping);
    }

    private void setupServerStubEmptyResponse(WireMockServer wireMockServer) {
        StubMapping stubMapping = wireMockServer.stubFor(
            get(urlPathEqualTo(ENDPOINT))
                .withHeader("Content-Type", equalTo("application/json"))
                .withQueryParam("id", matching("[0-9]+"))
                .withQueryParam("id2", matching("[0-9]+"))
                .willReturn(
                    aResponse()
                        .withBody(new byte[0])
                )
        );

        wireMockServer.addStubMapping(stubMapping);
    }

    private void setUpServerBodyStub(
            String methodName,
            WireMockServer wireMockServer,
            List<StringValuePattern> matchingJsonPaths) {
        setUpServerBodyStub(methodName, wireMockServer, matchingJsonPaths, null, null);
    }

    private void setUpServerBodyStub(
            String methodName,
            WireMockServer wireMockServer,
            List<StringValuePattern> matchingJsonPaths,
            String extraHeader,
            String expectedExtraHeaderValue) {

        MappingBuilder methodStub = (methodName.equalsIgnoreCase("PUT") ?
            put(urlEqualTo(ENDPOINT)) :
            post(urlEqualTo(ENDPOINT))
        );

        methodStub
            .withHeader("Content-Type", equalTo("application/json"));

        if (extraHeader != null && expectedExtraHeaderValue != null) {
            methodStub
                .withHeader(extraHeader, equalTo(expectedExtraHeaderValue));
        }

        // TODO think about writing custom matcher that will check node values against regexp
        //  or real values. Currently we check only if JsonPath exists. Also, we should check if
        // there are no extra fields.
        for (StringValuePattern pattern : matchingJsonPaths) {
            methodStub.withRequestBody(pattern);
        }

        methodStub
            .willReturn(
                aResponse()
                    .withTransformers(JsonTransform.NAME));

        StubMapping stubMapping = wireMockServer.stubFor(methodStub);

        wireMockServer.addStubMapping(stubMapping);
    }
}
