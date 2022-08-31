package com.getindata.connectors.http;

import java.util.HashSet;
import java.util.Set;

import org.apache.flink.configuration.ConfigOption;

import com.getindata.connectors.http.internal.sink.HttpSinkRequestEntry;

public class TestPostRequestCallbackFactory
    implements HttpPostRequestCallbackFactory<HttpSinkRequestEntry> {

    public static final String TEST_POST_REQUEST_CALLBACK_IDENT = "test-request-callback";

    @Override
    public HttpPostRequestCallback<HttpSinkRequestEntry> createHttpPostRequestCallback() {
        return new HttpPostRequestCallbackFactoryTest.TestPostRequestCallback();
    }

    @Override
    public String factoryIdentifier() { return TEST_POST_REQUEST_CALLBACK_IDENT; }

    @Override
    public Set<ConfigOption<?>> requiredOptions() { return new HashSet<>(); }

    @Override
    public Set<ConfigOption<?>> optionalOptions() { return new HashSet<>(); }
}
