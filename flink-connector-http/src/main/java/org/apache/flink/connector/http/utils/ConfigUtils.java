/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.http.utils;

import org.apache.flink.connector.http.config.ConfigException;
import org.apache.flink.connector.http.config.HttpConnectorConfigConstants;
import org.apache.flink.util.StringUtils;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.experimental.UtilityClass;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/** config utils. */
@UtilityClass
@NoArgsConstructor(access = AccessLevel.NONE)
public final class ConfigUtils {

    /**
     * A pattern matcher linebreak regexp that represents any Unicode linebreak sequence making it
     * effectively equivalent to.
     *
     * <pre>{@code
     * &#92;u000D&#92;u000A|[&#92;u000A&#92;u000B&#92;u000C&#92;u000D&#92;u0085&#92;u2028&#92;u2029]
     * }</pre>
     */
    public static final String UNIVERSAL_NEW_LINE_REGEXP = "\\R";

    private static final String PROPERTY_NAME_DELIMITER = ".";

    /**
     * Convert properties that name starts with given {@code keyPrefix} to Map. Values for this
     * property will be cast to {@code valueClazz} type.
     *
     * @param properties properties to extract keys from.
     * @param keyPrefix prefix used to match property name with.
     * @param valueClazz type to cast property values to.
     * @param <T> type of the elements of a returned map.
     * @return Map of propertyName to propertyValue.
     */
    public static <T> Map<String, T> propertiesToMap(
            Properties properties, String keyPrefix, Class<T> valueClazz) {

        Map<String, T> map = new HashMap<>();
        for (Map.Entry<Object, Object> entry : properties.entrySet()) {
            if (entry.getKey() instanceof String) {
                String key = (String) entry.getKey();
                if (key.startsWith(keyPrefix)) {
                    tryAddToConfigMap(properties, valueClazz, map, key);
                }
            } else {
                throw new ConfigException(
                        entry.getKey().toString(), entry.getValue(), "Key must be a string.");
            }
        }
        return map;
    }

    /**
     * A utility method to extract last element from property name. This method assumes property to
     * be in format as <b>{@code this.is.my.property.name}</b>, using "dot" as a delimiter. For this
     * example the returned value would be <b>{@code name}</b>.
     *
     * @param propertyKey Property name to extract the last element from.
     * @return property last element or the property name if {@code propertyKey} parameter had no
     *     dot delimiter.
     * @throws ConfigException when invalid property such as null, empty, blank, ended with dot was
     *     used.
     */
    public static String extractPropertyLastElement(String propertyKey) {
        if (StringUtils.isNullOrWhitespaceOnly(propertyKey)) {
            throw new ConfigException("Provided a property name that is null, empty or blank.");
        }

        if (!propertyKey.contains(PROPERTY_NAME_DELIMITER)) {
            return propertyKey;
        }

        int delimiterLastIndex = propertyKey.lastIndexOf(PROPERTY_NAME_DELIMITER);
        if (delimiterLastIndex == propertyKey.length() - 1) {
            throw new ConfigException(
                    String.format(
                            "Invalid property - %s. Property name should not end with property delimiter.",
                            propertyKey));
        }

        return propertyKey.substring(delimiterLastIndex + 1);
    }

    private static <T> void tryAddToConfigMap(
            Properties properties, Class<T> clazz, Map<String, T> map, String key) {
        try {
            map.put(key, clazz.cast(properties.get(key)));
        } catch (ClassCastException e) {
            throw new ConfigException(
                    String.format("Unable to cast value for property %s to type %s", key, clazz),
                    e);
        }
    }

    public static Properties getHttpConnectorProperties(Map<String, String> tableOptions) {
        final Properties httpProperties = new Properties();

        tableOptions.entrySet().stream()
                .filter(
                        entry ->
                                entry.getKey()
                                        .startsWith(
                                                HttpConnectorConfigConstants.FLINK_CONNECTOR_HTTP))
                .forEach(entry -> httpProperties.put(entry.getKey(), entry.getValue()));

        return httpProperties;
    }
}
