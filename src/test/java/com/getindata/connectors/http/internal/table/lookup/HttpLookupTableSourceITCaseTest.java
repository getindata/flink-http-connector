package com.getindata.connectors.http.internal.table.lookup;

import java.util.Comparator;
import java.util.Objects;
import java.util.SortedSet;
import java.util.TreeSet;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.stubbing.StubMapping;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;

@Slf4j
public class HttpLookupTableSourceITCaseTest {

    private static final int SERVER_PORT = 9090;

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
        wireMockServer = new WireMockServer(
            WireMockConfiguration.wireMockConfig()
                .port(SERVER_PORT)
                .extensions(JsonTransform.class)
        );
        wireMockServer.start();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        tEnv = StreamTableEnvironment.create(env);
    }

    @AfterEach
    public void tearDown() {
        wireMockServer.stop();
    }

    @Test
    public void testHttpDynamicSinkDefaultPost() throws Exception {

        setupServerStub(wireMockServer);

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
                + "'asyncPolling' = 'true'"
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
            get(urlPathEqualTo("/client"))
                .willReturn(
                    aResponse()
                        .withTransformers(JsonTransform.NAME)));

        wireMockServer.addStubMapping(stubMapping);
    }
}
