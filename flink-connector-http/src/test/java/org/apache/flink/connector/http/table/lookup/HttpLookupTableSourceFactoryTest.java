/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.http.table.lookup;

import org.apache.flink.connector.http.WireMockServerPortAllocator;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.connector.source.DynamicTableSource;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.table.factories.utils.FactoryMocks.createTableSource;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.junit.jupiter.api.Assertions.assertFalse;

/** Test for {@link HttpLookupTableSourceFactory}. */
public class HttpLookupTableSourceFactoryTest {

    private static final ResolvedSchema SCHEMA =
            new ResolvedSchema(
                    Arrays.asList(
                            Column.physical("id", DataTypes.STRING().notNull()),
                            Column.physical("msg", DataTypes.STRING().notNull()),
                            Column.physical("uuid", DataTypes.STRING().notNull()),
                            Column.physical(
                                    "details",
                                    DataTypes.ROW(
                                                    DataTypes.FIELD(
                                                            "isActive", DataTypes.BOOLEAN()),
                                                    DataTypes.FIELD(
                                                            "nestedDetails",
                                                            DataTypes.ROW(
                                                                    DataTypes.FIELD(
                                                                            "balance",
                                                                            DataTypes.STRING()))))
                                            .notNull())),
                    Collections.emptyList(),
                    UniqueConstraint.primaryKey("id", List.of("id")));

    @Test
    void validateHttpLookupSourceOptions() {

        HttpLookupTableSourceFactory httpLookupTableSourceFactory =
                new HttpLookupTableSourceFactory();
        TableConfig tableConfig = new TableConfig();
        httpLookupTableSourceFactory.validateHttpLookupSourceOptions(tableConfig);
        tableConfig.set(
                HttpLookupConnectorOptions.SOURCE_LOOKUP_OIDC_AUTH_TOKEN_ENDPOINT_URL.key(), "aaa");

        try {
            httpLookupTableSourceFactory.validateHttpLookupSourceOptions(tableConfig);
            assertFalse(true, "Expected an error.");
        } catch (IllegalArgumentException e) {
            // expected
        }
        // should now work.
        tableConfig.set(
                HttpLookupConnectorOptions.SOURCE_LOOKUP_OIDC_AUTH_TOKEN_REQUEST.key(), "bbb");

        httpLookupTableSourceFactory.validateHttpLookupSourceOptions(tableConfig);
    }

    @Test
    void shouldCreateForMandatoryFields() {
        Map<String, String> options = getMandatoryOptions();
        DynamicTableSource source = createTableSource(SCHEMA, options);
        assertThat(source).isNotNull();
        assertThat(source).isInstanceOf(HttpLookupTableSource.class);
    }

    @Test
    void shouldThrowIfMissingUrl() {
        Map<String, String> options = Collections.singletonMap("connector", "rest-lookup");
        assertThatExceptionOfType(ValidationException.class)
                .isThrownBy(() -> createTableSource(SCHEMA, options));
    }

    @Test
    void shouldAcceptWithUrlArgs() {
        Map<String, String> options = getOptions(Map.of("url-args", "id;msg"));
        DynamicTableSource source = createTableSource(SCHEMA, options);
        assertThat(source).isNotNull();
        assertThat(source).isInstanceOf(HttpLookupTableSource.class);
    }

    @Test
    void shouldHandleEmptyUrlArgs() {
        Map<String, String> options = getOptions(Collections.emptyMap());
        DynamicTableSource source = createTableSource(SCHEMA, options);
        assertThat(source).isNotNull();
        assertThat(source).isInstanceOf(HttpLookupTableSource.class);
    }

    private Map<String, String> getMandatoryOptions() {
        return Map.of(
                "connector", "rest-lookup",
                "url", "http://localhost:" + WireMockServerPortAllocator.PORT_BASE + "/service",
                "format", "json");
    }

    private Map<String, String> getOptions(Map<String, String> optionalOptions) {
        if (optionalOptions.isEmpty()) {
            return getMandatoryOptions();
        }

        Map<String, String> allOptions = new HashMap<>();
        allOptions.putAll(getMandatoryOptions());
        allOptions.putAll(optionalOptions);

        return allOptions;
    }
}
