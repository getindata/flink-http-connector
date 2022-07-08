package com.getindata.connectors.http.internal.table.lookup;

import java.io.Serializable;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.FunctionContext;

import com.getindata.connectors.http.internal.JsonResultTableConverter.HttpResultConverterOptions;
import com.getindata.connectors.http.internal.PollingClient;
import com.getindata.connectors.http.internal.PollingClientFactory;

public abstract class AbstractTablePollingClientFactory
    implements PollingClientFactory<RowData>, Serializable {

    @Override
    public PollingClient<RowData> createPollClient(FunctionContext context,
        HttpLookupConfig options) {

        HttpResultConverterOptions converterOptions =
            HttpResultConverterOptions.builder()
                .root(options.getRoot())
                .aliases(options.getAliasPaths())
                .columnNames(options.getColumnNames())
                .build();

        return createRowDataPollClient(options, converterOptions);
    }

    protected abstract PollingClient<RowData> createRowDataPollClient(
        HttpLookupConfig options, HttpResultConverterOptions converterOptions);
}
