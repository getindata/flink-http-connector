package com.getindata.connectors.http.internal.table.sink;

import java.util.HashSet;
import java.util.Set;

import org.apache.flink.configuration.ConfigOption;

import com.getindata.connectors.http.HttpPostRequestCallback;
import com.getindata.connectors.http.HttpPostRequestCallbackFactory;
import com.getindata.connectors.http.internal.sink.httpclient.HttpRequest;

/**
 * Factory for creating {@link Slf4jHttpPostRequestCallback}.
 */
public class Slf4jHttpPostRequestCallbackFactory
    implements HttpPostRequestCallbackFactory<HttpRequest> {

    public static final String IDENTIFIER = "slf4j-logger";

    @Override
    public HttpPostRequestCallback<HttpRequest> createHttpPostRequestCallback() {
        return new Slf4jHttpPostRequestCallback();
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return new HashSet<>();
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return new HashSet<>();
    }
}
