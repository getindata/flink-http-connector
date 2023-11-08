package com.getindata.connectors.http.internal.table.lookup.querycreators;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.FallbackKey;

/**
 * This is a ConfigOption that has an associate config option and prefix.
 *
 * Note that this Class used to extend ConfigOption,
 * but at Flink 1.16, there was a new way of doing class loaders
 * for custom content, so we could no longer extend ConfigOption.
 */
public class PrefixedConfigOption<T> {

    /**
     * Prefix that will be added to original option key.
     */
    private final String prefixedKey;
    /**
     * configOption to decorate
     */
    private ConfigOption configOption;

    public ConfigOption getConfigOption() {
        return configOption;
    }

    /**
     * @param keyPrefix prefix that will be added to decorated {@link ConfigOption} key.
     * @param other     original {@link ConfigOption} to decorate.
     */
    public PrefixedConfigOption(String keyPrefix, ConfigOption<T> other) {
        this.configOption = other;
        this.prefixedKey = keyPrefix + other.key();
        // This is not ideal...
        // We need to be able to update the ConfigOption's key, but it is a private variable.
        // Using reflection we can update the key field.
        try {
            Field  field = other.getClass().getDeclaredField("key");
            field.setAccessible(true);
            field.set(other, this.prefixedKey);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    private static FallbackKey[] getFallbackKeys(ConfigOption<?> other) {
        List<FallbackKey> fallbackKeys = new ArrayList<>();
        for (FallbackKey fallbackKey : other.fallbackKeys()) {
            fallbackKeys.add(fallbackKey);
        }
        return fallbackKeys.toArray(new FallbackKey[0]);
    }
}
