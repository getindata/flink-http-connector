/*
 * © Copyright IBM Corp. 2025
 */
package com.getindata.connectors.http.internal.table.lookup.querycreators;

import java.util.List;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.*;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.factories.DynamicTableFactory;
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
import static com.getindata.connectors.http.internal.table.lookup.querycreators.QueryCreatorUtils.getTableContext;

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

        this.config.setString("lookup-request.format", CustomJsonFormatFactory.IDENTIFIER);
        this.config.setString(
                String.format("lookup-request.format.%s.%s", CustomJsonFormatFactory.IDENTIFIER,
                        CustomJsonFormatFactory.REQUIRED_OPTION), "optionValue");
        this.config.set(REQUEST_QUERY_PARAM_FIELDS, List.of("key1"));
        // with sync
        createUsingFactory(false);
        // with async
        createUsingFactory(true);
    }

    @Test
    public void lookupQueryInfoTestRequiredConfig() {
        GenericJsonAndUrlQueryCreatorFactory genericJsonAndUrlQueryCreatorFactory =
                new GenericJsonAndUrlQueryCreatorFactory();
        assertThrows(RuntimeException.class, () -> {
            genericJsonAndUrlQueryCreatorFactory.createLookupQueryCreator(config,
                    null,
                    null);
        });
        // do not specify REQUEST_ARG_PATHS_CONFIG
        assertThrows(RuntimeException.class, () -> {
            genericJsonAndUrlQueryCreatorFactory.createLookupQueryCreator(config,
                    null,
                    null);
        });
    }

    private void createUsingFactory(boolean async) {
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

        LookupQueryInfo lookupQueryInfo = lookupQueryCreator.createLookupQuery(lookupRowData);
        assertThat(CustomJsonFormatFactory.requiredOptionsWereUsed).isTrue();
        assertThat(lookupQueryInfo.hasLookupQuery()).isTrue();
        assertThat(lookupQueryInfo.hasBodyBasedUrlQueryParameters()).isFalse();
        assertThat(lookupQueryInfo.hasPathBasedUrlParameters()).isFalse();
    }
    @Test
    void optionsTests() {
        GenericJsonAndUrlQueryCreatorFactory factory = new GenericJsonAndUrlQueryCreatorFactory();
        assertThat(factory.requiredOptions()).isEmpty();
        assertThat(factory.optionalOptions()).contains(REQUEST_QUERY_PARAM_FIELDS);
        assertThat(factory.optionalOptions()).contains(REQUEST_BODY_FIELDS);
        assertThat(factory.optionalOptions()).contains(REQUEST_URL_MAP);
    }
}
