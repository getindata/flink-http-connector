package com.getindata.connectors.http.internal.table.lookup;

import java.net.http.HttpClient;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.table.data.RowData;

import com.getindata.connectors.http.internal.PollingClient;
import com.getindata.connectors.http.internal.PollingClientFactory;

public class RestTablePollingClientFactory implements PollingClientFactory<RowData> {

    @Override
    public PollingClient<RowData> createPollClient(
            HttpLookupConfig options,
            DeserializationSchema<RowData> schemaDecoder) {

        return new RestTablePollingClient(HttpClient.newHttpClient(), schemaDecoder, options);
    }
}
