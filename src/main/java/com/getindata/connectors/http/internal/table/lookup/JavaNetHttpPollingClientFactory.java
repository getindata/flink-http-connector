package com.getindata.connectors.http.internal.table.lookup;

import java.net.http.HttpClient;
import java.net.http.HttpClient.Redirect;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.table.data.RowData;

import com.getindata.connectors.http.internal.PollingClient;
import com.getindata.connectors.http.internal.PollingClientFactory;

public class JavaNetHttpPollingClientFactory implements PollingClientFactory<RowData> {

    @Override
    public PollingClient<RowData> createPollClient(
            HttpLookupConfig options,
            DeserializationSchema<RowData> schemaDecoder) {

        HttpClient httpClient = HttpClient.newBuilder()
            .followRedirects(Redirect.NORMAL)
            .build();

        return new JavaNetHttpPollingClient(httpClient, schemaDecoder, options);
    }
}
