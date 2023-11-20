package com.getindata.connectors.http.internal.table.lookup.querycreators;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.FallbackKey;

/**
 * This is a ConfigOption that has an associated config option and prefix.
 *
 * Note that this Class used to extend ConfigOption,
 * but at Flink 1.16, there was a new way of doing class loaders
 * for custom content, so we could no longer extend ConfigOption.
 */
public class PrefixedConfigOption<T> {
    /**
     * configOption to decorate
     */
    private ConfigOption configOption;

    public ConfigOption getConfigOption() {
        return configOption;
    }

    /**
     * This constructor creates a new clone of the supplied option 'other' with
     * the prefix prefixing the key. We create a new object, because we do
     * not want to mutate a Flink object that we did not create.
     *
     * @param keyPrefix prefix that will be added to decorate the {@link ConfigOption} key.
     * @param other  original {@link ConfigOption} to clone and decorate.
     */
    public PrefixedConfigOption(String keyPrefix, ConfigOption<T> other) {
        String prefixedKey = keyPrefix + other.key();
        Class clazz;
        boolean isList;

        try {
            // get clazz
            Field field = other.getClass().getDeclaredField("clazz");
            field.setAccessible(true);
            clazz = (Class) field.get(other);

            // get isList
            field = other.getClass().getDeclaredField("isList");
            field.setAccessible(true);
            isList = (Boolean) field.get(other);

            /*
            * Create a new ConfigOption based on other, but with a prefixed key.
            * At 1.16 we cannot access the protected fields / constructor in the supplied
            * configOption as this object is loaded using a different classloader.
            * Without changing Flink to make the constructor, methods and fields public, we need
            * to use reflection to access and create the new prefixed ConfigOption. It is not
            * great practise to use reflection, but getting round this classloader issue
            * necessitates it's use.
            */
            Constructor constructor = other.getClass().getDeclaredConstructors()[0];
            constructor.setAccessible(true);
            configOption = (ConfigOption) constructor.newInstance(prefixedKey,
                    clazz,
                    other.description(),
                    other.defaultValue(),
                    isList,
                    getFallbackKeys(other));
        } catch (InstantiationException |
                 IllegalAccessException |
                 InvocationTargetException |
                 NoSuchFieldException e) {
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
