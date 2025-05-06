package com.getindata.connectors.http.internal.table.lookup;

import java.net.http.HttpClient;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.ConfigurationException;

import com.getindata.connectors.http.internal.PollingClientFactory;
import com.getindata.connectors.http.internal.utils.JavaNetHttpClientFactory;

public class JavaNetHttpPollingClientFactory implements PollingClientFactory<RowData> {

    private final HttpRequestFactory requestFactory;

    public JavaNetHttpPollingClientFactory(HttpRequestFactory requestFactory) {
        this.requestFactory = requestFactory;
    }

    @Override
    public JavaNetHttpPollingClient createPollClient(
            HttpLookupConfig options,
            DeserializationSchema<RowData> schemaDecoder) throws ConfigurationException {

        HttpClient httpClient = JavaNetHttpClientFactory.createClient(options);

        return new JavaNetHttpPollingClient(
            httpClient,
            schemaDecoder,
            options,
            requestFactory
        );
    }
}
