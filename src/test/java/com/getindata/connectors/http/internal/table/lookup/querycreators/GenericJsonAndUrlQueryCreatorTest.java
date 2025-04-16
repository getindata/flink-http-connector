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
<<<<<<< HEAD
import org.apache.flink.table.types.FieldsDataType;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.getindata.connectors.http.internal.table.lookup.LookupQueryInfo;
=======
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

>>>>>>> 6c68722 (HTTP-99 Generic Json url query creator)
import com.getindata.connectors.http.internal.table.lookup.LookupRow;
import com.getindata.connectors.http.internal.table.lookup.RowDataSingleValueLookupSchemaEntry;
import static com.getindata.connectors.http.internal.table.lookup.HttpLookupConnectorOptions.LOOKUP_METHOD;
import static com.getindata.connectors.http.internal.table.lookup.HttpLookupTableSourceFactory.row;
<<<<<<< HEAD
import static com.getindata.connectors.http.internal.table.lookup.querycreators.GenericJsonAndUrlQueryCreatorFactoryTest.getTableContext;

class GenericJsonAndUrlQueryCreatorTest {
    private static final String KEY = "key1";
    private static final String VALUE = "val1";
    // for GET this is the minimum config
    private static final List<String> QUERY_PARAMS = List.of(KEY);
    // Path param ArgPath required a stringified json object. As we have PersonBean
    // we can use that.
    private static final Map<String, String> URL_PARAMS = Map.of(KEY, KEY);
    private static final DataType dataType = row(List.of(
            DataTypes.FIELD(KEY, DataTypes.STRING())
    ));
    private static final ResolvedSchema resolvedSchema = ResolvedSchema.of(Column.physical(KEY,
            DataTypes.STRING()));
    private static final RowData ROWDATA = getRowData(1, VALUE);
    @ParameterizedTest
    @ValueSource(strings = {"GET", "PUT", "POST" })
    public void createLookupQueryTestStrAllOps(String operation) {
        // WHEN
        LookupRow lookupRow = getLookupRow();
        Configuration config = getConfiguration(operation);
        GenericJsonAndUrlQueryCreator universalJsonQueryCreator =
                (GenericJsonAndUrlQueryCreator) new GenericJsonAndUrlQueryCreatorFactory()
                        .createLookupQueryCreator(
                                config,
                                lookupRow,
                                getTableContext(config,
                                        resolvedSchema)
                        );
        var createdQuery = universalJsonQueryCreator.createLookupQuery(ROWDATA);
        // WHEN/THEN
        if (operation.equals("GET")) {
            validateCreatedQueryForGet(createdQuery);
        } else {
            validateCreatedQueryForPutAndPost(createdQuery);
        }
        // validate url based parameters
        assertThat(createdQuery.getPathBasedUrlParameters().size() == 1).isTrue();
        assertThat(createdQuery.getPathBasedUrlParameters().get(KEY)).isEqualTo(VALUE);
    }

    private static void validateCreatedQueryForGet( LookupQueryInfo createdQuery) {
        // check there is no body params and we have the expected lookup query
        assertThat(createdQuery.getBodyBasedUrlQueryParameters()).isEmpty();
        assertThat(createdQuery.getLookupQuery()).isEqualTo(KEY + "=" + VALUE);
    }
    private static void validateCreatedQueryForPutAndPost(LookupQueryInfo createdQuery) {
        // check we have the expected body params and lookup query
        assertThat(createdQuery
                .getBodyBasedUrlQueryParameters())
                .isEqualTo(KEY + "=" + VALUE);
        assertThat(createdQuery.getLookupQuery()).isEqualTo(
                "{\""
                         + KEY
                         + "\":\"" + VALUE
                         + "\"}");
    }

    private static @NotNull GenericRowData getRowData(int arity, String value) {
        var row = new GenericRowData(arity);
        row.setField(0, StringData.fromString(value));
        return row;
    }

    private static @NotNull Configuration getConfiguration(String operation) {
        Configuration config = new Configuration();
        config.set(GenericJsonAndUrlQueryCreatorFactory.REQUEST_QUERY_PARAM_FIELDS,
                QUERY_PARAMS);
        if (!operation.equals("GET")) {
            // add the body content for PUT and POST
            config.set(GenericJsonAndUrlQueryCreatorFactory.REQUEST_BODY_FIELDS,
                    QUERY_PARAMS);
        }
        config.set(GenericJsonAndUrlQueryCreatorFactory.REQUEST_URL_MAP, URL_PARAMS);
        config.setString(LOOKUP_METHOD, operation);
        return config;
    }

    private static @NotNull LookupRow getLookupRow() {
        LookupRow lookupRow = new LookupRow()
                .addLookupEntry(
                        new RowDataSingleValueLookupSchemaEntry(
                                KEY,
                                RowData.createFieldGetter(
                                        DataTypes.STRING().getLogicalType(), 0)
                        ));
        lookupRow.setLookupPhysicalRowDataType(dataType);
        return lookupRow;
    }

    @Test
    public void createLookupQueryTest() {
        // GIVEN
        List<String> query_params = List.of("key1", "key2");
        String operation = "GET";
        final String key1 = "key1";
        final String key2 = "key2";
        final String value = "val1";
=======
import static com.getindata.connectors.http.internal.table.lookup.querycreators.QueryCreatorUtils.getTableContext;

class GenericJsonAndUrlQueryCreatorTest {
    enum Operation {
        Get("GET"),
        Put("PUT"),
        Post("POST")
        ;
        public final String label;
        Operation() {
            this("GET");
        }

        Operation(String label) {
            this.label = label;
        }
    }
    @Test
    public void createLookupQueryTestStrAllOps() {
        for (Operation op: Operation.values()) {
            String operation = op.label;
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
            if (!op.equals("GET")) {
                // add the body content for PUT and POST
                config.set(GenericJsonAndUrlQueryCreatorFactory.REQUEST_BODY_FIELDS,
                        query_params);
            }
            config.set(GenericJsonAndUrlQueryCreatorFactory.REQUEST_URL_MAP, url_params);
            config.setString(LOOKUP_METHOD, operation);
            // GIVEN

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
    }
    @Test
    public void createLookupQueryTest() {
        List<String> query_params = List.of("key1", "key2");
        String operation = "GET";
        String key1 = "key1";
        String key2 = "key2";
        String value = "val1";
>>>>>>> 6c68722 (HTTP-99 Generic Json url query creator)
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
<<<<<<< HEAD
        ));
=======
        ));;
>>>>>>> 6c68722 (HTTP-99 Generic Json url query creator)
        ResolvedSchema resolvedSchema = ResolvedSchema.of(
                Column.physical(key1, DataTypes.STRING()),
                Column.physical(key2, DataTypes.STRING()));
        Configuration config = new Configuration();
        config.set(GenericJsonAndUrlQueryCreatorFactory.REQUEST_QUERY_PARAM_FIELDS, query_params);
        config.set(GenericJsonAndUrlQueryCreatorFactory.REQUEST_URL_MAP, url_params);
        config.setString(LOOKUP_METHOD, operation);
<<<<<<< HEAD
        lookupRow.setLookupPhysicalRowDataType(dataType);
        // WHEN
=======
        // GIVEN

        lookupRow.setLookupPhysicalRowDataType(dataType);

>>>>>>> 6c68722 (HTTP-99 Generic Json url query creator)
        GenericJsonAndUrlQueryCreator genericJsonAndUrlQueryCreator =
                (GenericJsonAndUrlQueryCreator) new GenericJsonAndUrlQueryCreatorFactory()
                        .createLookupQueryCreator(
                                config,
                                lookupRow,
                                getTableContext(config,
                                        resolvedSchema)
                        );
<<<<<<< HEAD
        var row = getRowData(2, value);
=======
        var row = new GenericRowData(2);
        row.setField(0, StringData.fromString(value));
>>>>>>> 6c68722 (HTTP-99 Generic Json url query creator)
        row.setField(1, StringData.fromString(value));
        var createdQuery = genericJsonAndUrlQueryCreator.createLookupQuery(row);
        // THEN
        assertThat(createdQuery.getPathBasedUrlParameters().get("AAA")).isEqualTo(value);
        assertThat(createdQuery.getBodyBasedUrlQueryParameters()).isEmpty();
        assertThat(createdQuery.getLookupQuery()).isEqualTo(key1 + "=" + value
                   + "&" + key2 + "=" + value);
    }
    @Test
<<<<<<< HEAD
    public void failSerializationOpenTest() {
        List<String> paths_config =List.of("key1");
        final String operation = "GET";
        final String key = "key1";
=======
    public void failserializationOpenTest() {
        List<String> paths_config =List.of("key1");
        String operation = "GET";
        String key = "key1";
        String value = "val1";
>>>>>>> 6c68722 (HTTP-99 Generic Json url query creator)

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
<<<<<<< HEAD
=======
        // GIVEN
>>>>>>> 6c68722 (HTTP-99 Generic Json url query creator)

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
<<<<<<< HEAD
        // GIVEN
        ObjectMapper mapper = ObjectMapperAdapter.instance();
        PersonBean person = new PersonBean("aaa", "bbb");
        // WHEN
        JsonNode personNode = mapper.valueToTree(person);
        // THEN
=======
        ObjectMapper mapper = ObjectMapperAdapter.instance();
        PersonBean person = new PersonBean("aaa", "bbb");
        JsonNode personNode = mapper.valueToTree(person);
>>>>>>> 6c68722 (HTTP-99 Generic Json url query creator)
        assertThrows(RuntimeException.class, () -> {
            GenericJsonAndUrlQueryCreator.convertToQueryParameters(
                    (ObjectNode) personNode, "bad encoding");
        });
    }
    @Test void rowDataToRowTest() {
<<<<<<< HEAD
        // GIVEN
        // String
        final String key1 = "key1";
        final String key2 = "key2";
        final String key3 = "key3";
        final String value = "value";
        int intValue = 10;
        GenericRowData rowData = GenericRowData.of(
                StringData.fromString(value),
=======
        // String
        String KEY1 = "key1";
        String KEY2 = "key2";
        String KEY3 = "key3";
        String VALUE = "value";
        int intValue = 10;
        GenericRowData rowData = GenericRowData.of(
                StringData.fromString(VALUE),
>>>>>>> 6c68722 (HTTP-99 Generic Json url query creator)
                intValue,
                intValue
        );
        DataType dataType = row(List.of(
<<<<<<< HEAD
                DataTypes.FIELD(key1, DataTypes.STRING()),
                DataTypes.FIELD(key2, DataTypes.DATE()),
                DataTypes.FIELD(key3, DataTypes.TIMESTAMP_LTZ())
        ));
        // WHEN
        Row row = rowDataToRow(rowData, dataType);
        // THEN
        assertThat(row.getField(key1).equals(value));
        assertThat(row.getField(key2).equals("1970-01-01T00:00:00.010"));
        assertThat(row.getField(key3).equals("1970-01-01T00:00:00.010Z"));
    }
    /**
     * Create a Row from a RowData and DataType
     * @param lookupRowData the lookup RowData
     * @param rowType the datatype
     * @return row return row
     */
    private static Row rowDataToRow(final RowData lookupRowData, final DataType rowType) {
        Preconditions.checkNotNull(lookupRowData);
        Preconditions.checkNotNull(rowType);

        final Row row = Row.withNames();
        final List<DataTypes.Field> rowFields = FieldsDataType.getFields(rowType);

        for (int idx = 0; idx < rowFields.size(); idx++) {
            final String fieldName = rowFields.get(idx).getName();
            final Object fieldValue = ((GenericRowData) lookupRowData).getField(idx);
            row.setField(fieldName, fieldValue);
        }
        return row;
=======
                DataTypes.FIELD(KEY1, DataTypes.STRING()),
                DataTypes.FIELD(KEY2, DataTypes.DATE()),
                DataTypes.FIELD(KEY3, DataTypes.TIMESTAMP_LTZ())
        ));
        Row row =  GenericJsonAndUrlQueryCreator.rowDataToRow(rowData, dataType);
        assertThat(row.getField(KEY1).equals(VALUE));
        assertThat(row.getField(KEY2).equals("1970-01-01T00:00:00.010"));
        assertThat(row.getField(KEY3).equals("1970-01-01T00:00:00.010Z"));
>>>>>>> 6c68722 (HTTP-99 Generic Json url query creator)
    }
}
