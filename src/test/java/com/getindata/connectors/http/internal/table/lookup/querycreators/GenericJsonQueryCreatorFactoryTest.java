package com.getindata.connectors.http.internal.table.lookup.querycreators;

import java.util.Collections;
import java.util.List;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.*;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;

import com.getindata.connectors.http.internal.table.lookup.LookupRow;
import com.getindata.connectors.http.internal.table.lookup.RowDataSingleValueLookupSchemaEntry;
import static com.getindata.connectors.http.internal.table.lookup.HttpLookupTableSourceFactory.row;

class GenericJsonQueryCreatorFactoryTest {

    private Configuration config;
    private LookupRow lookupRow;

    private DynamicTableFactory.Context tableContext;

    @BeforeEach
    public void setUp() {
        this.config = new Configuration();
        this.lookupRow = new LookupRow();
        lookupRow = new LookupRow()
            .addLookupEntry(
                new RowDataSingleValueLookupSchemaEntry(
                    "key1",
                    RowData.createFieldGetter(DataTypes.STRING().getLogicalType(), 0)
                ));

        lookupRow.setLookupPhysicalRowDataType(
            row(List.of(
                DataTypes.FIELD("key1", DataTypes.STRING())
            )));

        CustomFormatFactory.requiredOptionsWereUsed = false;

        ResolvedSchema resolvedSchema =
                ResolvedSchema.of(Column.physical("key1", DataTypes.STRING()));

        this.tableContext = new FactoryUtil.DefaultDynamicTableContext(
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

    @Test
    public void shouldPassPropertiesToQueryCreatorFormat() {
        assertThat(CustomFormatFactory.requiredOptionsWereUsed)
            .withFailMessage(
                "CustomFormatFactory was not cleared, "
                    + "make sure `CustomFormatFactory.requiredOptionsWereUsed = false` "
                    + "was called before this test execution.")
            .isFalse();

        this.config.setString("lookup-request.format", CustomFormatFactory.IDENTIFIER);
        this.config.setString(
            String.format("lookup-request.format.%s.%s", CustomFormatFactory.IDENTIFIER,
                CustomFormatFactory.REQUIRED_OPTION), "optionValue");

        new GenericJsonQueryCreatorFactory().createLookupQueryCreator(
            config,
            lookupRow,
            tableContext
        );

        assertThat(CustomFormatFactory.requiredOptionsWereUsed).isTrue();
    }
}
