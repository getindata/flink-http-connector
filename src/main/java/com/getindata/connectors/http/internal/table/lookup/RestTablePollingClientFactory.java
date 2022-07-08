package com.getindata.connectors.http.internal.table.lookup;

import java.net.http.HttpClient;

import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.table.data.RowData;

import com.getindata.connectors.http.internal.JsonResultTableConverter;
import com.getindata.connectors.http.internal.JsonResultTableConverter.HttpResultConverterOptions;
import com.getindata.connectors.http.internal.PollingClient;

public class RestTablePollingClientFactory extends AbstractTablePollingClientFactory {

    @Override
    protected PollingClient<RowData> createRowDataPollClient(
        HttpLookupConfig options, HttpResultConverterOptions converterOptions) {
        return new RestTablePollingClient(new JsonResultTableConverter(converterOptions), options,
            HttpClient.newHttpClient());
    }

    @Override
    public PollingClient<RowData> createPollClient(SourceReaderContext readerContext) {
        return new RestTablePollingClient(new JsonResultTableConverter(null), null,
            HttpClient.newHttpClient());
    }
}
