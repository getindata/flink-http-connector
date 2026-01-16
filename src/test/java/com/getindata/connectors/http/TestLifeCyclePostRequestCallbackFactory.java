package com.getindata.connectors.http;

import java.util.HashSet;
import java.util.Set;

import org.apache.flink.configuration.ConfigOption;

import com.getindata.connectors.http.internal.sink.httpclient.HttpRequest;

public class TestLifeCyclePostRequestCallbackFactory implements HttpPostRequestCallbackFactory<HttpRequest> {

    public static final String TEST_LIFE_CYCLE_POST_REQUEST_CALLBACK_IDENT = "test-lifecycle-request-callback";

    @Override
    public HttpPostRequestCallback<HttpRequest> createHttpPostRequestCallback() {
        return new HttpPostRequestCallbackFactoryTest.TestLifeCyclePostRequestCallback();
    }

    @Override
    public String factoryIdentifier() { return TEST_LIFE_CYCLE_POST_REQUEST_CALLBACK_IDENT; }

    @Override
    public Set<ConfigOption<?>> requiredOptions() { return new HashSet<>(); }

    @Override
    public Set<ConfigOption<?>> optionalOptions() { return new HashSet<>(); }
}
