/*
 * © Copyright IBM Corp. 2025
 */
package com.getindata.connectors.http.internal.table.lookup.querycreators;

<<<<<<< HEAD
import java.util.Collections;
=======
>>>>>>> 6c68722 (HTTP-99 Generic Json url query creator)
import java.util.List;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.DataTypes;
<<<<<<< HEAD
import org.apache.flink.table.api.Schema;
=======
>>>>>>> 6c68722 (HTTP-99 Generic Json url query creator)
import org.apache.flink.table.catalog.*;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.factories.DynamicTableFactory;
<<<<<<< HEAD
import org.apache.flink.table.factories.FactoryUtil;
=======
>>>>>>> 6c68722 (HTTP-99 Generic Json url query creator)
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.getindata.connectors.http.LookupQueryCreator;
import com.getindata.connectors.http.internal.table.lookup.HttpLookupConnectorOptions;
import com.getindata.connectors.http.internal.table.lookup.LookupQueryInfo;
import com.getindata.connectors.http.internal.table.lookup.LookupRow;
import com.getindata.connectors.http.internal.table.lookup.RowDataSingleValueLookupSchemaEntry;
import static com.getindata.connectors.http.internal.table.lookup.HttpLookupTableSourceFactory.row;
import static com.getindata.connectors.http.internal.table.lookup.querycreators.GenericJsonAndUrlQueryCreatorFactory.*;
<<<<<<< HEAD
=======
import static com.getindata.connectors.http.internal.table.lookup.querycreators.QueryCreatorUtils.getTableContext;
>>>>>>> 6c68722 (HTTP-99 Generic Json url query creator)

class GenericJsonAndUrlQueryCreatorFactoryTest
{
    private Configuration config = new Configuration();

    private DynamicTableFactory.Context tableContext;

    @BeforeEach
    public void setUp() {
        CustomJsonFormatFactory.requiredOptionsWereUsed = false;
        this.tableContext = getTableContext(this.config, ResolvedSchema.of(Column.physical("key1",
                DataTypes.STRING())));
    }

    @Test
    public void lookupQueryInfoTestStr() {
        assertThat(CustomJsonFormatFactory.requiredOptionsWereUsed)
                .withFailMessage(
                        "CustomJsonFormat was not cleared, "
                                + "make sure `CustomJsonFormatFactory.requiredOptionsWereUsed"
                                + "= false` "
                                + "was called before this test execution.")
                .isFalse();
<<<<<<< HEAD
        // GIVEN
=======

>>>>>>> 6c68722 (HTTP-99 Generic Json url query creator)
        this.config.setString("lookup-request.format", CustomJsonFormatFactory.IDENTIFIER);
        this.config.setString(
                String.format("lookup-request.format.%s.%s", CustomJsonFormatFactory.IDENTIFIER,
                        CustomJsonFormatFactory.REQUIRED_OPTION), "optionValue");
        this.config.set(REQUEST_QUERY_PARAM_FIELDS, List.of("key1"));
<<<<<<< HEAD
        // WHEN/THEN
=======
>>>>>>> 6c68722 (HTTP-99 Generic Json url query creator)
        // with sync
        createUsingFactory(false);
        // with async
        createUsingFactory(true);
    }

    @Test
    public void lookupQueryInfoTestRequiredConfig() {
<<<<<<< HEAD
        //GIVEN
        GenericJsonAndUrlQueryCreatorFactory genericJsonAndUrlQueryCreatorFactory =
                new GenericJsonAndUrlQueryCreatorFactory();
        // WHEN/THEN
=======
        GenericJsonAndUrlQueryCreatorFactory genericJsonAndUrlQueryCreatorFactory =
                new GenericJsonAndUrlQueryCreatorFactory();
        assertThrows(RuntimeException.class, () -> {
            genericJsonAndUrlQueryCreatorFactory.createLookupQueryCreator(config,
                    null,
                    null);
        });
        // do not specify REQUEST_ARG_PATHS_CONFIG
>>>>>>> 6c68722 (HTTP-99 Generic Json url query creator)
        assertThrows(RuntimeException.class, () -> {
            genericJsonAndUrlQueryCreatorFactory.createLookupQueryCreator(config,
                    null,
                    null);
        });
    }

    private void createUsingFactory(boolean async) {
<<<<<<< HEAD
        // GIVEN
=======
>>>>>>> 6c68722 (HTTP-99 Generic Json url query creator)
        this.config.setBoolean(HttpLookupConnectorOptions.ASYNC_POLLING, async);
        LookupRow lookupRow= new LookupRow()
                .addLookupEntry(
                        new RowDataSingleValueLookupSchemaEntry(
                                "key1",
                                RowData.createFieldGetter(DataTypes.STRING().getLogicalType(), 0)
                        ));

        lookupRow.setLookupPhysicalRowDataType(
                row(List.of(
                        DataTypes.FIELD("key1", DataTypes.STRING())
                )));
        LookupQueryCreator lookupQueryCreator = new
                GenericJsonAndUrlQueryCreatorFactory().createLookupQueryCreator(
                config,
                lookupRow,
                tableContext
        );
        GenericRowData lookupRowData = GenericRowData.of(
                StringData.fromString("val1")
        );
<<<<<<< HEAD
        // WHEN/THEN
=======

>>>>>>> 6c68722 (HTTP-99 Generic Json url query creator)
        LookupQueryInfo lookupQueryInfo = lookupQueryCreator.createLookupQuery(lookupRowData);
        assertThat(CustomJsonFormatFactory.requiredOptionsWereUsed).isTrue();
        assertThat(lookupQueryInfo.hasLookupQuery()).isTrue();
        assertThat(lookupQueryInfo.hasBodyBasedUrlQueryParameters()).isFalse();
        assertThat(lookupQueryInfo.hasPathBasedUrlParameters()).isFalse();
    }
    @Test
    void optionsTests() {
<<<<<<< HEAD
        // GIVEN
        GenericJsonAndUrlQueryCreatorFactory factory = new GenericJsonAndUrlQueryCreatorFactory();
        // WHEN/THEN
=======
        GenericJsonAndUrlQueryCreatorFactory factory = new GenericJsonAndUrlQueryCreatorFactory();
>>>>>>> 6c68722 (HTTP-99 Generic Json url query creator)
        assertThat(factory.requiredOptions()).isEmpty();
        assertThat(factory.optionalOptions()).contains(REQUEST_QUERY_PARAM_FIELDS);
        assertThat(factory.optionalOptions()).contains(REQUEST_BODY_FIELDS);
        assertThat(factory.optionalOptions()).contains(REQUEST_URL_MAP);
    }
<<<<<<< HEAD

    public static DynamicTableFactory.Context getTableContext(Configuration config,
                                                              ResolvedSchema resolvedSchema) {
        return new FactoryUtil.DefaultDynamicTableContext(
                ObjectIdentifier.of("default", "default", "test"),
                new ResolvedCatalogTable(
                        CatalogTable.of(
                                Schema.newBuilder().fromResolvedSchema(resolvedSchema).build(),
                                null,
                                Collections.emptyList(),
                                Collections.emptyMap()),
                        resolvedSchema),
                Collections.emptyMap(),
                config,
                Thread.currentThread().getContextClassLoader(),
                false
        );
    }
=======
>>>>>>> 6c68722 (HTTP-99 Generic Json url query creator)
}
