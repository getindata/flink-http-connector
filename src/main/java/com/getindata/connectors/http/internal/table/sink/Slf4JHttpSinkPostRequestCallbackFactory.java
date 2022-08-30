package com.getindata.connectors.http.internal.table.sink;

import java.util.HashSet;
import java.util.Set;

import org.apache.flink.configuration.ConfigOption;

import com.getindata.connectors.http.HttpSinkPostRequestCallback;
import com.getindata.connectors.http.HttpSinkPostRequestCallbackFactory;

public class Slf4JHttpSinkPostRequestCallbackFactory implements HttpSinkPostRequestCallbackFactory {
    public static final String IDENTIFIER = "slf4j-logger";

    @Override
    public HttpSinkPostRequestCallback createHttpSinkPostRequestCallback() {
        return new Slf4jHttpSinkPostRequestCallback();
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
