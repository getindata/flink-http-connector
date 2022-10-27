package com.getindata.connectors.http.internal.table.lookup;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.connector.source.AsyncTableFunctionProvider;
import org.apache.flink.table.connector.source.TableFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.connector.source.LookupRuntimeProviderContext;
import org.apache.flink.table.types.DataType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import static org.apache.flink.table.factories.utils.FactoryMocks.createTableSource;
import static org.assertj.core.api.Assertions.assertThat;

import static com.getindata.connectors.http.internal.table.lookup.HttpLookupTableSourceFactory.row;

class HttpLookupTableSourceTest {

    public static final DataType PHYSICAL_ROW_DATA_TYPE =
        row(List.of(DataTypes.FIELD("id", DataTypes.STRING().notNull())));

    private static final ResolvedSchema SCHEMA =
        new ResolvedSchema(
            Arrays.asList(
                Column.physical("id", DataTypes.STRING().notNull()),
                Column.physical("msg", DataTypes.STRING().notNull()),
                Column.physical("uuid", DataTypes.STRING().notNull()),
                Column.physical("details", DataTypes.ROW(
                    DataTypes.FIELD("isActive", DataTypes.BOOLEAN()),
                    DataTypes.FIELD("nestedDetails", DataTypes.ROW(
                            DataTypes.FIELD("balance", DataTypes.STRING())
                        )
                    )
                ).notNull())
            ),
            Collections.emptyList(),
            UniqueConstraint.primaryKey("id", List.of("id"))
        );

    // lookupKey index {{0}} means first column.
    private final int[][] lookupKey = {{0}};

    @BeforeEach
    public void setUp() {

        LookupRow expectedLookupRow = new LookupRow();
        expectedLookupRow.addLookupEntry(
            new RowDataSingleValueLookupSchemaEntry(
                "id",
                RowData.createFieldGetter(DataTypes.STRING().notNull().getLogicalType(), 0)
            )
        );
        expectedLookupRow.setLookupPhysicalRowDataType(PHYSICAL_ROW_DATA_TYPE);
    }

    @Test
    @SuppressWarnings("unchecked")
    void shouldCreateTableSourceWithParams() {
        HttpLookupTableSource tableSource =
            (HttpLookupTableSource) createTableSource(SCHEMA, getOptions());

        TableFunctionProvider<RowData> lookupProvider =
            (TableFunctionProvider<RowData>)
                tableSource.getLookupRuntimeProvider(new LookupRuntimeProviderContext(lookupKey));

        HttpTableLookupFunction tableFunction =
            (HttpTableLookupFunction) lookupProvider.createTableFunction();

        LookupRow actualLookupRow = tableFunction.getLookupRow();
        assertThat(actualLookupRow).isNotNull();
        assertThat(actualLookupRow.getLookupEntries()).isNotEmpty();
        assertThat(actualLookupRow.getLookupPhysicalRowDataType())
            .isEqualTo(PHYSICAL_ROW_DATA_TYPE);

        HttpLookupConfig actualLookupConfig = tableFunction.getOptions();
        assertThat(actualLookupConfig).isNotNull();
        assertThat(
            actualLookupConfig.getReadableConfig().get(
                ConfigOptions.key("connector").stringType().noDefaultValue())
        )
            .withFailMessage(
                "Readable config probably was not passed from Table Factory or it is empty.")
            .isNotNull();
    }

    @Test
    @SuppressWarnings("unchecked")
    void shouldCreateAsyncTableSourceWithParams() {
        Map<String, String> options = getOptionsWithAsync();

        HttpLookupTableSource tableSource =
            (HttpLookupTableSource) createTableSource(SCHEMA, options);

        AsyncTableFunctionProvider<RowData> lookupProvider =
            (AsyncTableFunctionProvider<RowData>)
                tableSource.getLookupRuntimeProvider(new LookupRuntimeProviderContext(lookupKey));

        AsyncHttpTableLookupFunction tableFunction =
            (AsyncHttpTableLookupFunction) lookupProvider.createAsyncTableFunction();

        LookupRow actualLookupRow = tableFunction.getLookupRow();
        assertThat(actualLookupRow).isNotNull();
        assertThat(actualLookupRow.getLookupEntries()).isNotEmpty();
        assertThat(actualLookupRow.getLookupPhysicalRowDataType())
            .isEqualTo(PHYSICAL_ROW_DATA_TYPE);

        HttpLookupConfig actualLookupConfig = tableFunction.getOptions();
        assertThat(actualLookupConfig).isNotNull();
        assertThat(actualLookupConfig.isUseAsync()).isTrue();
        assertThat(
            actualLookupConfig.getReadableConfig().get(HttpLookupConnectorOptions.ASYNC_POLLING)
        )
            .withFailMessage(
                "Readable config probably was not passed from Table Factory or it is empty.")
            .isTrue();
    }

    private Map<String, String> getOptionsWithAsync() {
        Map<String, String> options = getOptions();
        options = new HashMap<>(options);
        options.put("asyncPolling", "true");
        return options;
    }

    private Map<String, String> getOptions() {
        return Map.of(
            "connector", "rest-lookup",
            "url", "http://localhost:8080/service",
            "format", "json");
    }
}
