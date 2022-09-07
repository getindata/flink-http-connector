package com.getindata.connectors.http.internal.table.lookup;

import java.net.http.HttpClient;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.table.data.RowData;

import com.getindata.connectors.http.internal.PollingClientFactory;
import com.getindata.connectors.http.internal.utils.JavaNetHttpClientFactory;

public class JavaNetHttpPollingClientFactory implements PollingClientFactory<RowData> {

    @Override
    public JavaNetHttpPollingClient createPollClient(
            HttpLookupConfig options,
            DeserializationSchema<RowData> schemaDecoder) {

        HttpClient httpClient = JavaNetHttpClientFactory.createClient(options.getProperties());

        return new JavaNetHttpPollingClient(httpClient, schemaDecoder, options);
    }
}
