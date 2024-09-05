package com.getindata.connectors.http.internal.table.lookup;

import java.net.http.HttpClient;

import com.getindata.connectors.http.internal.PollingClientFactory;
import com.getindata.connectors.http.internal.utils.JavaNetHttpClientFactory;

public class JavaNetHttpPollingClientFactory implements PollingClientFactory<byte[]> {

    @Override
    public JavaNetHttpPollingClient createPollClient(HttpLookupConfig options) {
        HttpClient httpClient = JavaNetHttpClientFactory.createClient(options.getProperties());
        return new JavaNetHttpPollingClient(httpClient, options);
    }
}
