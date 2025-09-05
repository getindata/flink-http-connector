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

import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.connector.http.WireMockServerPortAllocator;
import org.apache.flink.metrics.groups.CacheMetricGroup;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.connector.source.lookup.AsyncLookupFunctionProvider;
import org.apache.flink.table.connector.source.lookup.LookupFunctionProvider;
import org.apache.flink.table.connector.source.lookup.PartialCachingAsyncLookupProvider;
import org.apache.flink.table.connector.source.lookup.PartialCachingLookupProvider;
import org.apache.flink.table.connector.source.lookup.cache.LookupCache;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.connector.source.LookupRuntimeProviderContext;
import org.apache.flink.table.types.DataType;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.connector.http.table.lookup.HttpLookupTableSourceFactory.row;
import static org.apache.flink.table.factories.utils.FactoryMocks.createTableSource;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Test for {@link HttpLookupTableSource}. */
class HttpLookupTableSourceTest {

    public static final DataType PHYSICAL_ROW_DATA_TYPE =
            row(List.of(DataTypes.FIELD("id", DataTypes.STRING().notNull())));

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

    // lookupKey index {{0}} means first column.
    private final int[][] lookupKey = {{0}};

    @BeforeEach
    public void setUp() {

        LookupRow expectedLookupRow = new LookupRow();
        expectedLookupRow.addLookupEntry(
                new RowDataSingleValueLookupSchemaEntry(
                        "id",
                        RowData.createFieldGetter(
                                DataTypes.STRING().notNull().getLogicalType(), 0)));
        expectedLookupRow.setLookupPhysicalRowDataType(PHYSICAL_ROW_DATA_TYPE);
    }

    @Test
    @SuppressWarnings("unchecked")
    void shouldCreateTableSourceWithParams() {
        HttpLookupTableSource tableSource =
                (HttpLookupTableSource) createTableSource(SCHEMA, getOptions());

        LookupTableSource.LookupRuntimeProvider lookupProvider =
                tableSource.getLookupRuntimeProvider(new LookupRuntimeProviderContext(lookupKey));
        HttpTableLookupFunction tableFunction =
                (HttpTableLookupFunction)
                        ((LookupFunctionProvider) lookupProvider).createLookupFunction();

        LookupRow actualLookupRow = tableFunction.getLookupRow();
        assertThat(actualLookupRow).isNotNull();
        assertThat(actualLookupRow.getLookupEntries()).isNotEmpty();
        assertThat(actualLookupRow.getLookupPhysicalRowDataType())
                .isEqualTo(PHYSICAL_ROW_DATA_TYPE);

        HttpLookupConfig actualLookupConfig = tableFunction.getOptions();
        assertThat(actualLookupConfig).isNotNull();
        assertThat(
                        actualLookupConfig
                                .getReadableConfig()
                                .get(ConfigOptions.key("connector").stringType().noDefaultValue()))
                .withFailMessage(
                        "Readable config probably was not passed from Table Factory or it is empty.")
                .isNotNull();
    }

    @Test
    @SuppressWarnings("unchecked")
    void shouldCreateAsyncTableSourceWithParams() {
        Map<String, String> options = getOptionsWithAsync();

        HttpLookupTableSource tableSource =
                (HttpLookupTableSource) createTableSource(SCHEMA, options);

        AsyncLookupFunctionProvider lookupProvider =
                (AsyncLookupFunctionProvider)
                        tableSource.getLookupRuntimeProvider(
                                new LookupRuntimeProviderContext(lookupKey));

        AsyncHttpTableLookupFunction tableFunction =
                (AsyncHttpTableLookupFunction) lookupProvider.createAsyncLookupFunction();

        LookupRow actualLookupRow = tableFunction.getLookupRow();
        assertThat(actualLookupRow).isNotNull();
        assertThat(actualLookupRow.getLookupEntries()).isNotEmpty();
        assertThat(actualLookupRow.getLookupPhysicalRowDataType())
                .isEqualTo(PHYSICAL_ROW_DATA_TYPE);

        HttpLookupConfig actualLookupConfig = tableFunction.getOptions();
        assertThat(actualLookupConfig).isNotNull();
        assertThat(actualLookupConfig.isUseAsync()).isTrue();
        assertThat(
                        actualLookupConfig
                                .getReadableConfig()
                                .get(HttpLookupConnectorOptions.ASYNC_POLLING))
                .withFailMessage(
                        "Readable config probably was not passed"
                                + " from Table Factory or it is empty.")
                .isTrue();
    }

    @ParameterizedTest
    @MethodSource("configProvider")
    void testGetLookupRuntimeProvider(TestSpec testSpec) {
        LookupCache cache =
                new LookupCache() {
                    @Override
                    public void open(CacheMetricGroup cacheMetricGroup) {}

                    @Nullable
                    @Override
                    public Collection<RowData> getIfPresent(RowData rowData) {
                        return null;
                    }

                    @Override
                    public Collection<RowData> put(
                            RowData rowData, Collection<RowData> collection) {
                        return null;
                    }

                    @Override
                    public void invalidate(RowData rowData) {}

                    @Override
                    public long size() {
                        return 0;
                    }

                    @Override
                    public void close() throws Exception {}
                };

        HttpLookupConfig options = HttpLookupConfig.builder().useAsync(testSpec.isAsync).build();
        LookupTableSource.LookupRuntimeProvider lookupRuntimeProvider =
                getLookupRuntimeProvider(testSpec.hasCache ? cache : null, options);
        assertTrue(testSpec.expected.isInstance(lookupRuntimeProvider));
    }

    private static class TestSpec {

        boolean hasCache;
        boolean isAsync;

        Class expected;

        private TestSpec(boolean hasCache, boolean isAsync, Class expected) {
            this.hasCache = hasCache;
            this.isAsync = isAsync;
            this.expected = expected;
        }

        @Override
        public String toString() {
            return "TestSpec{"
                    + "hasCache="
                    + hasCache
                    + ", isAsync="
                    + isAsync
                    + ", expected="
                    + expected
                    + '}';
        }
    }

    static Collection<TestSpec> configProvider() {
        return List.of(
                new TestSpec(false, false, LookupFunctionProvider.class),
                new TestSpec(true, false, PartialCachingLookupProvider.class),
                new TestSpec(false, true, AsyncLookupFunctionProvider.class),
                new TestSpec(true, true, PartialCachingAsyncLookupProvider.class));
    }

    private static LookupTableSource.LookupRuntimeProvider getLookupRuntimeProvider(
            LookupCache cache, HttpLookupConfig options) {
        HttpLookupTableSource tableSource =
                new HttpLookupTableSource(null, options, null, null, cache);
        int[][] lookupKeys = {{1, 2}};
        LookupTableSource.LookupContext lookupContext =
                new LookupRuntimeProviderContext(lookupKeys);
        return tableSource.getLookupRuntimeProvider(null, null, null);
    }

    private Map<String, String> getOptionsWithAsync() {
        Map<String, String> options = getOptions();
        options = new HashMap<>(options);
        options.put("asyncPolling", "true");
        return options;
    }

    private Map<String, String> getOptions() {
        return Map.of(
                "connector", "rest-lookup",
                "url", "http://localhost:" + WireMockServerPortAllocator.PORT_BASE + "/service",
                "format", "json");
    }
}
