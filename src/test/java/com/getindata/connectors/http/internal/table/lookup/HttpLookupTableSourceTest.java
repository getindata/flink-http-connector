package com.getindata.connectors.http.internal.table.lookup;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.connector.source.AsyncTableFunctionProvider;
import org.apache.flink.table.connector.source.TableFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.connector.source.LookupRuntimeProviderContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import static org.apache.flink.table.factories.utils.FactoryMocks.createTableSource;
import static org.assertj.core.api.Assertions.assertThat;

import com.getindata.connectors.http.internal.table.lookup.HttpTableLookupFunction.ColumnData;

class HttpLookupTableSourceTest {

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

    private final int[][] lookupKey = {{0}};

    private ColumnData expectedColumnData;

    private HttpLookupConfig expectedLookupConfig;

    @BeforeEach
    public void setUp() {
        expectedColumnData =
            ColumnData.builder().keyNames(List.of("id").toArray(new String[1])).build();

        expectedLookupConfig =
            HttpLookupConfig.builder()
                .url("http://localhost:8080/service")
                .build();
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

        assertThat(tableFunction.getColumnData()).isEqualTo(expectedColumnData);
        assertThat(tableFunction.getOptions()).isEqualTo(expectedLookupConfig);
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

        expectedLookupConfig =
            HttpLookupConfig.builder()
                .useAsync(true)
                .url("http://localhost:8080/service")
                .build();

        assertThat(tableFunction.getColumnData()).isEqualTo(expectedColumnData);
        assertThat(tableFunction.getOptions()).isEqualTo(expectedLookupConfig);
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
