package com.getindata.connectors.http.internal.table.lookup;

import java.util.HashSet;
import java.util.Set;

import org.apache.flink.configuration.ConfigOption;

import com.getindata.connectors.http.HttpPostRequestCallback;
import com.getindata.connectors.http.HttpPostRequestCallbackFactory;

/**
 * Factory for creating {@link Slf4JHttpLookupPostRequestCallback}.
 */
public class Slf4jHttpLookupPostRequestCallbackFactory
    implements HttpPostRequestCallbackFactory<HttpLookupSourceRequestEntry> {

    public static final String IDENTIFIER = "slf4j-lookup-logger";

    @Override
    public HttpPostRequestCallback<HttpLookupSourceRequestEntry> createHttpPostRequestCallback() {
        return new Slf4JHttpLookupPostRequestCallback();
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
