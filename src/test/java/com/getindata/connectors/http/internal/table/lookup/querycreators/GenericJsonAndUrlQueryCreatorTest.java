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
import com.getindata.connectors.http.internal.table.lookup.LookupRow;
import com.getindata.connectors.http.internal.table.lookup.RowDataSingleValueLookupSchemaEntry;
import static com.getindata.connectors.http.internal.table.lookup.HttpLookupConnectorOptions.LOOKUP_METHOD;
import static com.getindata.connectors.http.internal.table.lookup.HttpLookupTableSourceFactory.row;
import static com.getindata.connectors.http.internal.table.lookup.querycreators.QueryCreatorUtils.getTableContext;

class GenericJsonAndUrlQueryCreatorTest {
    private static final String KEY_1 = "key1";
    private static final String KEY_2 = "key2";
    private static final String KEY_3 = "key3";
    private static final String VALUE = "val1";
    // for GET this is the minimum config
    private static final List<String> QUERY_PARAMS = List.of(KEY_1);
    // Path param ArgPath required a stringified json object. As we have PersonBean
    // we can use that.
    private static final Map<String, String> URL_PARAMS = Map.of(KEY_1, KEY_1);
    private static final DataType DATATYPE_1 = row(List.of(
            DataTypes.FIELD(KEY_1, DataTypes.STRING())
    ));
    private static final DataType DATATYPE_1_2 = row(List.of(
            DataTypes.FIELD(KEY_1, DataTypes.STRING()),
            DataTypes.FIELD(KEY_2, DataTypes.STRING())
    ));
    private static final ResolvedSchema RESOLVED_SCHEMA = ResolvedSchema.of(Column.physical(KEY_1,
            DataTypes.STRING()));
    private static final RowData ROWDATA = getRowData(1, VALUE);

    @ParameterizedTest
    @ValueSource(strings = {"GET", "PUT", "POST" })
    public void createLookupQueryTestStrAllOps(String operation) {
        //GIVEN
        LookupRow lookupRow = getLookupRow(KEY_1);
        Configuration config = getConfiguration(operation);
        GenericJsonAndUrlQueryCreator universalJsonQueryCreator =
                (GenericJsonAndUrlQueryCreator) new GenericJsonAndUrlQueryCreatorFactory()
                        .createLookupQueryCreator(
                                config,
                                lookupRow,
                                getTableContext(config,
                                        RESOLVED_SCHEMA)
                        );
        // WHEN
        var createdQuery = universalJsonQueryCreator.createLookupQuery(ROWDATA);
        // THEN
        if (operation.equals("GET")) {
            validateCreatedQueryForGet(createdQuery);
        } else {
            validateCreatedQueryForPutAndPost(createdQuery);
        }
        // validate url based parameters
        assertThat(createdQuery.getPathBasedUrlParameters().size() == 1).isTrue();
        assertThat(createdQuery.getPathBasedUrlParameters().get(KEY_1)).isEqualTo(VALUE);
    }

    @Test
    public void createLookupQueryTest() {
        // GIVEN
        List<String> query_params = List.of(KEY_1, KEY_2);
        final String URL_INSERT = "AAA";
        Map<String, String> url_params = Map.of(KEY_1, URL_INSERT);
        LookupRow lookupRow = getLookupRow(KEY_1, KEY_2);
        ResolvedSchema resolvedSchema = ResolvedSchema.of(
                Column.physical(KEY_1, DataTypes.STRING()),
                Column.physical(KEY_2, DataTypes.STRING()));
        Configuration config = getConfiguration("GET");
        config.set(GenericJsonAndUrlQueryCreatorFactory.REQUEST_QUERY_PARAM_FIELDS, query_params);
        config.set(GenericJsonAndUrlQueryCreatorFactory.REQUEST_URL_MAP, url_params);
        lookupRow.setLookupPhysicalRowDataType(DATATYPE_1_2);
        GenericJsonAndUrlQueryCreator genericJsonAndUrlQueryCreator =
                (GenericJsonAndUrlQueryCreator) new GenericJsonAndUrlQueryCreatorFactory()
                        .createLookupQueryCreator(
                                config,
                                lookupRow,
                                getTableContext(config,
                                        resolvedSchema)
                        );
        var row = getRowData(2, VALUE);
        row.setField(1, StringData.fromString(VALUE));
        // WHEN
        var createdQuery = genericJsonAndUrlQueryCreator.createLookupQuery(row);
        // THEN
        assertThat(createdQuery.getPathBasedUrlParameters().get(URL_INSERT)).isEqualTo(VALUE);
        assertThat(createdQuery.getBodyBasedUrlQueryParameters()).isEmpty();
        assertThat(createdQuery.getLookupQuery()).isEqualTo(KEY_1 + "=" + VALUE
                + "&" + KEY_2 + "=" + VALUE);
    }

    @Test
    public void failSerializationOpenTest() {
        // GIVEN
        LookupRow lookupRow = getLookupRow(KEY_1);
        ResolvedSchema resolvedSchema = ResolvedSchema.of(Column.physical(KEY_1,
                DataTypes.STRING()));
        Configuration config = getConfiguration("GET");
        lookupRow.setLookupPhysicalRowDataType(DATATYPE_1);
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
        // WHEN
        genericJsonAndUrlQueryCreator.setSerializationSchema(mockSerialiationSchema);
        var row = new GenericRowData(1);
        // THEN
        assertThrows(RuntimeException.class, () -> {
            genericJsonAndUrlQueryCreator.createLookupQuery(row);
        });
    }
    @Test
    void convertToQueryParametersUnsupportedEncodingTest() {
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
    @Test
    void rowDataToRowTest() {
        // GIVEN
        // String
        final String value = VALUE;
        int intValue = 10;
        GenericRowData rowData = GenericRowData.of(
                StringData.fromString(value),
                intValue,
                intValue
        );
        DataType dataType = row(List.of(
                DataTypes.FIELD(KEY_1, DataTypes.STRING()),
                DataTypes.FIELD(KEY_2, DataTypes.DATE()),
                DataTypes.FIELD(KEY_3, DataTypes.TIMESTAMP_LTZ())
        ));
        // WHEN
        Row row = rowDataToRow(rowData, dataType);
        // THEN
        assertThat(row.getField(KEY_1).equals(value));
        assertThat(row.getField(KEY_2).equals("1970-01-01T00:00:00.010"));
        assertThat(row.getField(KEY_3).equals("1970-01-01T00:00:00.010Z"));
    }

    private static void validateCreatedQueryForGet( LookupQueryInfo createdQuery) {
        // check there is no body params and we have the expected lookup query
        assertThat(createdQuery.getBodyBasedUrlQueryParameters()).isEmpty();
        assertThat(createdQuery.getLookupQuery()).isEqualTo(KEY_1 + "=" + VALUE);
    }

    private static void validateCreatedQueryForPutAndPost(LookupQueryInfo createdQuery) {
        // check we have the expected body params and lookup query
        assertThat(createdQuery
                .getBodyBasedUrlQueryParameters())
                .isEqualTo(KEY_1 + "=" + VALUE);
        assertThat(createdQuery.getLookupQuery()).isEqualTo(
                "{\""
                         + KEY_1
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

    private static @NotNull LookupRow getLookupRow(String... keys ) {

        LookupRow lookupRow = new LookupRow();
        for (int keyNumber = 0; keyNumber < keys.length; keyNumber++) {
            lookupRow.addLookupEntry(
                     new RowDataSingleValueLookupSchemaEntry(
                              keys[keyNumber],
                              RowData.createFieldGetter(
                                    DataTypes.STRING().getLogicalType(), keyNumber)
                     ));
            lookupRow.setLookupPhysicalRowDataType(DATATYPE_1);
        }
        return lookupRow;
    }

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
    }
}
