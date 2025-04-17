/*
 * © Copyright IBM Corp. 2025
 */
package com.getindata.connectors.http.internal.table.lookup.querycreators;

import java.util.List;
import java.util.Map;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.getindata.connectors.http.internal.table.lookup.LookupRow;
import com.getindata.connectors.http.internal.table.lookup.RowDataSingleValueLookupSchemaEntry;
import static com.getindata.connectors.http.internal.table.lookup.HttpLookupConnectorOptions.LOOKUP_METHOD;
import static com.getindata.connectors.http.internal.table.lookup.HttpLookupTableSourceFactory.row;
import static com.getindata.connectors.http.internal.table.lookup.querycreators.QueryCreatorUtils.getTableContext;

class GenericJsonAndUrlQueryCreatorTest {

    @ParameterizedTest
    @ValueSource(strings = {"GET", "PUT", "POST" })
    public void createLookupQueryTestStrAllOps(String operation) {
        String key = "key1";
        String value = "val1";
        // for GET this is the minimum config
        List<String> query_params = List.of(key);
        // Path param ArgPath required a stringified json object. As we have PersonBean
        // we can use that.
        Map<String, String> url_params = Map.of(key, key);
        LookupRow lookupRow = new LookupRow()
                .addLookupEntry(
                        new RowDataSingleValueLookupSchemaEntry(
                                key,
                                RowData.createFieldGetter(
                                        DataTypes.STRING().getLogicalType(), 0)
                        ));
        DataType dataType = row(List.of(
                DataTypes.FIELD(key, DataTypes.STRING())
        ));
        lookupRow.setLookupPhysicalRowDataType(dataType);
        ResolvedSchema resolvedSchema = ResolvedSchema.of(Column.physical(key,
                DataTypes.STRING()));
        Configuration config = new Configuration();
        config.set(GenericJsonAndUrlQueryCreatorFactory.REQUEST_QUERY_PARAM_FIELDS,
                query_params);
        if (!operation.equals("GET")) {
            // add the body content for PUT and POST
            config.set(GenericJsonAndUrlQueryCreatorFactory.REQUEST_BODY_FIELDS,
                    query_params);
        }
        config.set(GenericJsonAndUrlQueryCreatorFactory.REQUEST_URL_MAP, url_params);
        config.setString(LOOKUP_METHOD, operation);
        GenericJsonAndUrlQueryCreator universalJsonQueryCreator =
                (GenericJsonAndUrlQueryCreator) new GenericJsonAndUrlQueryCreatorFactory()
                        .createLookupQueryCreator(
                                config,
                                lookupRow,
                                getTableContext(config,
                                        resolvedSchema)
                        );
        var row = new GenericRowData(1);
        row.setField(0, StringData.fromString(value));
        var createdQuery = universalJsonQueryCreator.createLookupQuery(row);
        // THEN
        if (operation.equals("GET")) {
            assertThat(createdQuery.getBodyBasedUrlQueryParameters()).isEmpty();
            assertThat(createdQuery.getLookupQuery()).isEqualTo(key + "=" + value);
        } else {
            assertThat(createdQuery
                    .getBodyBasedUrlQueryParameters())
                    .isEqualTo(key + "=" + value);
            assertThat(createdQuery.getLookupQuery()).isEqualTo(
                    "{\""
                            + key
                            + "\":\"" + value
                            + "\"}");
        }
        assertThat(createdQuery.getPathBasedUrlParameters().size() == 1).isTrue();
        assertThat(createdQuery.getPathBasedUrlParameters().get(key)).isEqualTo(value);
    }
    @Test
    public void createLookupQueryTest() {
        List<String> query_params = List.of("key1", "key2");
        String operation = "GET";
        String key1 = "key1";
        String key2 = "key2";
        String value = "val1";
        Map<String, String> url_params = Map.of(key1, "AAA");

        LookupRow lookupRow = new LookupRow()
                .addLookupEntry(
                        new RowDataSingleValueLookupSchemaEntry(
                                key1,
                                RowData.createFieldGetter(DataTypes.STRING().getLogicalType(), 0)
                        )
                );
        lookupRow.addLookupEntry(
                new RowDataSingleValueLookupSchemaEntry(
                        key2,
                        RowData.createFieldGetter(DataTypes.STRING().getLogicalType(), 1)));
        DataType dataType = row(List.of(
                DataTypes.FIELD(key1, DataTypes.STRING()),
                DataTypes.FIELD(key2, DataTypes.STRING())
        ));;
        ResolvedSchema resolvedSchema = ResolvedSchema.of(
                Column.physical(key1, DataTypes.STRING()),
                Column.physical(key2, DataTypes.STRING()));
        Configuration config = new Configuration();
        config.set(GenericJsonAndUrlQueryCreatorFactory.REQUEST_QUERY_PARAM_FIELDS, query_params);
        config.set(GenericJsonAndUrlQueryCreatorFactory.REQUEST_URL_MAP, url_params);
        config.setString(LOOKUP_METHOD, operation);
        // GIVEN

        lookupRow.setLookupPhysicalRowDataType(dataType);

        GenericJsonAndUrlQueryCreator genericJsonAndUrlQueryCreator =
                (GenericJsonAndUrlQueryCreator) new GenericJsonAndUrlQueryCreatorFactory()
                        .createLookupQueryCreator(
                                config,
                                lookupRow,
                                getTableContext(config,
                                        resolvedSchema)
                        );
        var row = new GenericRowData(2);
        row.setField(0, StringData.fromString(value));
        row.setField(1, StringData.fromString(value));
        var createdQuery = genericJsonAndUrlQueryCreator.createLookupQuery(row);
        // THEN
        assertThat(createdQuery.getPathBasedUrlParameters().get("AAA")).isEqualTo(value);
        assertThat(createdQuery.getBodyBasedUrlQueryParameters()).isEmpty();
        assertThat(createdQuery.getLookupQuery()).isEqualTo(key1 + "=" + value
                   + "&" + key2 + "=" + value);
    }
    @Test
    public void failserializationOpenTest() {
        List<String> paths_config =List.of("key1");
        String operation = "GET";
        String key = "key1";

        LookupRow lookupRow = new LookupRow()
                .addLookupEntry(
                        new RowDataSingleValueLookupSchemaEntry(
                                key,
                                RowData.createFieldGetter(DataTypes.STRING().getLogicalType(), 0)
                        ));
        DataType dataType = row(List.of(
                DataTypes.FIELD(key, DataTypes.STRING())
        ));
        ResolvedSchema resolvedSchema = ResolvedSchema.of(Column.physical(key,
                DataTypes.STRING()));
        Configuration config = new Configuration();
        config.set(GenericJsonAndUrlQueryCreatorFactory.REQUEST_QUERY_PARAM_FIELDS, paths_config);
        config.setString(LOOKUP_METHOD, operation);

        lookupRow.setLookupPhysicalRowDataType(dataType);

        GenericJsonAndUrlQueryCreator genericJsonAndUrlQueryCreator =
                (GenericJsonAndUrlQueryCreator) new GenericJsonAndUrlQueryCreatorFactory()
                        .createLookupQueryCreator(
                                config,
                                lookupRow,
                                getTableContext(config,
                                        resolvedSchema)
                        );
        // create a SerializationSchema that throws and exception in open
        SerializationSchema<RowData> mockSerialiationSchema = new SerializationSchema<RowData>() {
            @Override
            public void open(InitializationContext context) throws Exception {
                throw new Exception("Exception for testing");
            }
            @Override
            public byte[] serialize(RowData element) {
                return new byte[0];
            }
        };
        genericJsonAndUrlQueryCreator.setSerializationSchema(mockSerialiationSchema);
        var row = new GenericRowData(1);
        assertThrows(RuntimeException.class, () -> {
            genericJsonAndUrlQueryCreator.createLookupQuery(row);
        });
    }
    @Test void convertToQueryParametersUnsupportedEncodingTest() {
        // GIVEN
        ObjectMapper mapper = ObjectMapperAdapter.instance();
        PersonBean person = new PersonBean("aaa", "bbb");
        // WHEN
        JsonNode personNode = mapper.valueToTree(person);
        // THEN
        assertThrows(RuntimeException.class, () -> {
            GenericJsonAndUrlQueryCreator.convertToQueryParameters(
                    (ObjectNode) personNode, "bad encoding");
        });
    }
    @Test void rowDataToRowTest() {
        // GIVEN
        // String
        String KEY1 = "key1";
        String KEY2 = "key2";
        String KEY3 = "key3";
        String VALUE = "value";
        int intValue = 10;
        GenericRowData rowData = GenericRowData.of(
                StringData.fromString(VALUE),
                intValue,
                intValue
        );
        DataType dataType = row(List.of(
                DataTypes.FIELD(KEY1, DataTypes.STRING()),
                DataTypes.FIELD(KEY2, DataTypes.DATE()),
                DataTypes.FIELD(KEY3, DataTypes.TIMESTAMP_LTZ())
        ));
        // WHEN
        Row row =  GenericJsonAndUrlQueryCreator.rowDataToRow(rowData, dataType);
        // THEN
        assertThat(row.getField(KEY1).equals(VALUE));
        assertThat(row.getField(KEY2).equals("1970-01-01T00:00:00.010"));
        assertThat(row.getField(KEY3).equals("1970-01-01T00:00:00.010Z"));
    }
}
