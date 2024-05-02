package com.getindata.connectors.http;

import java.util.HashSet;
import java.util.Set;

import org.apache.flink.configuration.ConfigOption;

import com.getindata.connectors.http.internal.table.lookup.HttpLookupSourceRequestEntry;

public class TestLookupPostRequestCallbackFactory
        implements HttpPostRequestCallbackFactory<HttpLookupSourceRequestEntry> {

    public static final String TEST_LOOKUP_POST_REQUEST_CALLBACK_IDENT =
            "test-lookup-request-callback";

    @Override
    public HttpPostRequestCallback<HttpLookupSourceRequestEntry> createHttpPostRequestCallback() {
        return new HttpPostRequestCallbackFactoryTest.TestLookupPostRequestCallback();
    }

    @Override
    public String factoryIdentifier() { return TEST_LOOKUP_POST_REQUEST_CALLBACK_IDENT; }

    @Override
    public Set<ConfigOption<?>> requiredOptions() { return new HashSet<>(); }

    @Override
    public Set<ConfigOption<?>> optionalOptions() { return new HashSet<>(); }
}
