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

package org.apache.flink.connector.http.table.lookup.querycreators;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.factories.SerializationFormatFactory;

import java.util.Optional;

/**
 * An internal extension of Flink's {@link Configuration} class. This implementation uses {@link
 * PrefixedConfigOption} internally to decorate Flink's {@link ConfigOption} used for {@link
 * Configuration#getOptional(ConfigOption)} method.
 */
class QueryFormatAwareConfiguration extends Configuration {

    /**
     * Format name for {@link SerializationFormatFactory} identifier used as {@code
     * HttpLookupConnectorOptions#LOOKUP_REQUEST_FORMAT}.
     *
     * <p>This will be used as prefix parameter for {@link PrefixedConfigOption}.
     */
    private final String queryFormatName;

    QueryFormatAwareConfiguration(String queryFormatName, Configuration other) {
        super(other);
        this.queryFormatName =
                (queryFormatName.endsWith(".")) ? queryFormatName : queryFormatName + ".";
    }

    /**
     * Returns value for {@link ConfigOption} option which key is prefixed with "queryFormatName".
     *
     * @param option option which key will be prefixed with queryFormatName.
     * @return value for option after adding queryFormatName prefix
     */
    @Override
    public <T> Optional<T> getOptional(ConfigOption<T> option) {
        PrefixedConfigOption<T> prefixedConfigOption =
                new PrefixedConfigOption<>(queryFormatName, option);
        return super.getOptional(prefixedConfigOption.getConfigOption());
    }
}
