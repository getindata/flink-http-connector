package com.getindata.connectors.http.internal.table.lookup.querycreators;

import java.util.Set;

import org.apache.flink.configuration.ConfigOption;

import com.getindata.connectors.http.LookupQueryCreator;
import com.getindata.connectors.http.LookupQueryCreatorFactory;

/**
 * Factory for creating {@link GenericGetQueryCreator}.
 */
public class GenericGetQueryCreatorFactory implements LookupQueryCreatorFactory {
    public static final String IDENTIFIER = "generic-get-query";

    @Override
    public LookupQueryCreator createLookupQueryCreator() {
        return new GenericGetQueryCreator();
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return Set.of();
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return Set.of();
    }
}
