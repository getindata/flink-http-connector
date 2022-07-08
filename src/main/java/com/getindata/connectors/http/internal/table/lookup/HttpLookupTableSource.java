package com.getindata.connectors.http.internal.table.lookup;

import java.util.List;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.table.connector.source.AsyncTableFunctionProvider;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.connector.source.TableFunctionProvider;
import org.apache.flink.table.connector.source.abilities.SupportsLimitPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsProjectionPushDown;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;

import com.getindata.connectors.http.internal.PollingClientFactory;
import com.getindata.connectors.http.internal.table.lookup.HttpTableLookupFunction.ColumnData;

@Slf4j
@RequiredArgsConstructor
public class HttpLookupTableSource
    implements LookupTableSource, SupportsProjectionPushDown, SupportsLimitPushDown {

    private final DataType physicalRowDataType;
    private final PollingClientFactory<RowData> pollingClientFactory;
    private final HttpLookupConfig lookupConfig;

    @Override
    public LookupRuntimeProvider getLookupRuntimeProvider(LookupContext context) {
        String[] keyNames = new String[context.getKeys().length];
        List<String> fieldNames = TableSourceHelper.getFieldNames(physicalRowDataType);
        for (int i = 0; i < keyNames.length; i++) {
            int[] innerKeyArr = context.getKeys()[i];
            String fieldName = fieldNames.get(innerKeyArr[0]);
            keyNames[i] = fieldName;
        }

        return buildLookupFunction(keyNames);
    }

    @Override
    public DynamicTableSource copy() {
        return new HttpLookupTableSource(physicalRowDataType, pollingClientFactory, lookupConfig);
    }

    @Override
    public String asSummaryString() {
        return "Http Lookup Table Source";
    }

    @Override
    public void applyLimit(long limit) {
    }

    @Override
    public boolean supportsNestedProjection() {
        return false;
    }

    @Override
    public void applyProjection(int[][] projectedFields) {
    }

    private LookupRuntimeProvider buildLookupFunction(String[] keyNames) {
        ColumnData columnData = ColumnData.builder().keyNames(keyNames).build();

        HttpTableLookupFunction dataLookupFunction =
            HttpTableLookupFunction.builder()
                .pollingClientFactory(pollingClientFactory)
                .columnData(columnData)
                .options(lookupConfig)
                .build();

        if (lookupConfig.isUseAsync()) {
            log.info("Using Async version of HttpLookupTable.");
            return AsyncTableFunctionProvider.of(
                new AsyncHttpTableLookupFunction(dataLookupFunction));
        } else {
            log.info("Using blocking version of HttpLookupTable.");
            return TableFunctionProvider.of(dataLookupFunction);
        }
    }
}
