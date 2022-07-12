package com.getindata.connectors.http.internal.utils;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

import com.getindata.connectors.http.internal.config.ConfigException;

// TODO EXP-98 add Javadoc and tests
@NoArgsConstructor(access = AccessLevel.NONE)
public final class ConfigUtils {

    public static <T> Map<String, T> propertiesToMap(
            Properties properties,
            String keyPrefix,
            Class<T> clazz) {

        Map<String, T> map = new HashMap<>();
        for (Map.Entry<Object, Object> entry : properties.entrySet()) {
            if (entry.getKey() instanceof String) {
                String key = (String) entry.getKey();
                if (key.startsWith(keyPrefix)) {
                    tryAddToConfigMap(properties, clazz, map, key);
                }
            } else {
                throw new ConfigException(
                    entry.getKey().toString(),
                    entry.getValue(), "Key must be a string."
                );
            }
        }
        return map;
    }

    private static <T> void tryAddToConfigMap(
            Properties properties,
            Class<T> clazz, Map<String, T> map,
            String key) {
        try {
            map.put(key, clazz.cast(properties.get(key)));
        } catch (ClassCastException e) {
            throw new ConfigException(
                String.format("Unable to cast value for property %s to type %s", key,
                    clazz), e);
        }
    }

}
