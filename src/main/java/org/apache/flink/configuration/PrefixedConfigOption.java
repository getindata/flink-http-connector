package org.apache.flink.configuration;

import java.util.ArrayList;
import java.util.List;

import org.apache.flink.configuration.description.Description;

/**
 * Extension of Flink's {@link ConfigOption} class, where constant prefix is added to option key.
 * <p>
 * Since the original {@link ConfigOption} has no public constructor, in order to extend it we need
 * to use put this class into exact same package structure as {@link ConfigOption} is in. Thanks to
 * that, we can call {@link ConfigOption} package protected constructor.
 */
public class PrefixedConfigOption<T> extends ConfigOption<T> {

    /**
     * Prefix that will be added to original option key.
     */
    private final String prefixedKey;

    /**
     * @param keyPrefix prefix that will be added to decorated {@link ConfigOption} key.
     * @param other     original {@link ConfigOption} to decorate.
     */
    public PrefixedConfigOption(String keyPrefix, ConfigOption<T> other) {
        this(
            keyPrefix,
            other.key(),
            other.getClazz(),
            other.description(),
            other.defaultValue(),
            other.isList(),
            getFallbackKeys(other));
    }

    /**
     * Creates a new config option with fallback keys.
     *
     * @param key          The current key for that config option
     * @param clazz        describes type of the ConfigOption, see description of the clazz field
     * @param description  Description for that option
     * @param defaultValue The default value for this option
     * @param isList       tells if the ConfigOption describes a list option, see description of the
     *                     clazz field
     * @param fallbackKeys The list of fallback keys, in the order to be checked
     */
    PrefixedConfigOption(
            String keyPrefix,
            String key,
            Class<?> clazz,
            Description description,
            T defaultValue,
            boolean isList,
            FallbackKey... fallbackKeys) {

        super(key, clazz, description, defaultValue, isList, fallbackKeys);
        this.prefixedKey = keyPrefix + super.key();
    }

    private static FallbackKey[] getFallbackKeys(ConfigOption<?> other) {
        List<FallbackKey> fallbackKeys = new ArrayList<>();
        for (FallbackKey fallbackKey : other.fallbackKeys()) {
            fallbackKeys.add(fallbackKey);
        }
        return fallbackKeys.toArray(new FallbackKey[0]);
    }

    /**
     * Returns {@link ConfigOption} key with prefix defined for this {@link PrefixedConfigOption}
     * instance.
     *
     * @return Option key with prefix.
     */
    @Override
    public String key() {
        return prefixedKey;
    }

    @Override
    public String toString() {
        return String.format("Key: '%s' , default: %s", prefixedKey, defaultValue());
    }
}
