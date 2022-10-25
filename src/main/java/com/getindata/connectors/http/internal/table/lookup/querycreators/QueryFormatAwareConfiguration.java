package com.getindata.connectors.http.internal.table.lookup.querycreators;

import java.util.Optional;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.PrefixedConfigOption;
import org.apache.flink.table.factories.SerializationFormatFactory;

/**
 * An internal extension of Flink's {@link Configuration} class. This implementation uses {@link
 * PrefixedConfigOption} internally to decorate Flink's {@link ConfigOption} used for {@link
 * Configuration#getOptional(ConfigOption)} method.
 */
class QueryFormatAwareConfiguration extends Configuration {

    /**
     * Format name for {@link SerializationFormatFactory} identifier used as {@code
     * HttpLookupConnectorOptions#LOOKUP_REQUEST_FORMAT}.
     * <p>
     * This will be used as prefix parameter for {@link PrefixedConfigOption}.
     */
    private final String queryFormatName;

    QueryFormatAwareConfiguration(String queryFormatName, Configuration other) {
        super(other);
        this.queryFormatName =
            (queryFormatName.endsWith(".")) ? queryFormatName : queryFormatName + ".";
    }

    /**
     * Returns value for {@link ConfigOption} option which key is prefixed with "queryFormatName"
     *
     * @param option option which key will be prefixed with queryFormatName.
     * @return value for option after adding queryFormatName prefix
     */
    @Override
    public <T> Optional<T> getOptional(ConfigOption<T> option) {
        PrefixedConfigOption<T> configOption = new PrefixedConfigOption<>(queryFormatName, option);
        return super.getOptional(configOption);
    }

}
