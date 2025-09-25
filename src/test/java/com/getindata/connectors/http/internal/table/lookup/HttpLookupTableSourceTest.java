package com.getindata.connectors.http.internal.table.lookup;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpHeaders;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.*;

import javax.net.ssl.SSLSession;

import org.apache.flink.configuration.ConfigOptions;
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
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.runtime.connector.source.LookupRuntimeProviderContext;
import org.apache.flink.table.types.AtomicDataType;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.IntType;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.testcontainers.shaded.com.google.common.collect.ImmutableList;
import static org.apache.flink.table.factories.utils.FactoryMocks.createTableSource;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;

import static com.getindata.connectors.http.internal.table.lookup.HttpLookupTableSource.ReadableMetadata.ERROR_STRING;
import static com.getindata.connectors.http.internal.table.lookup.HttpLookupTableSource.ReadableMetadata.HTTP_COMPLETION_STATE;
import static com.getindata.connectors.http.internal.table.lookup.HttpLookupTableSource.ReadableMetadata.HTTP_HEADERS;
import static com.getindata.connectors.http.internal.table.lookup.HttpLookupTableSource.ReadableMetadata.HTTP_STATUS_CODE;
import static com.getindata.connectors.http.internal.table.lookup.HttpLookupTableSourceFactory.row;

class HttpLookupTableSourceTest {

    public static final DataType PHYSICAL_ROW_DATA_TYPE =
            row(List.of(DataTypes.FIELD("id", DataTypes.STRING().notNull())));

    private static final ResolvedSchema SCHEMA =
            new ResolvedSchema(
                    Arrays.asList(
                            Column.physical("id", DataTypes.STRING().notNull()),
                            Column.physical("msg", DataTypes.STRING().notNull()),
                            Column.physical("uuid", DataTypes.STRING().notNull()),
                            Column.physical("details", DataTypes.ROW(
                                    DataTypes.FIELD("isActive", DataTypes.BOOLEAN()),
                                    DataTypes.FIELD("nestedDetails", DataTypes.ROW(
                                                    DataTypes.FIELD("balance", DataTypes.STRING())
                                            )
                                    )
                            ).notNull())
                    ),
                    Collections.emptyList(),
                    UniqueConstraint.primaryKey("id", List.of("id"))
            );

    // lookupKey index {{0}} means first column.
    private final int[][] lookupKey = {{0}};

    @BeforeEach
    public void setUp() {

        LookupRow expectedLookupRow = new LookupRow();
        expectedLookupRow.addLookupEntry(
                new RowDataSingleValueLookupSchemaEntry(
                        "id",
                        RowData.createFieldGetter(DataTypes.STRING().notNull().getLogicalType(), 0)
                )
        );
        expectedLookupRow.setLookupPhysicalRowDataType(PHYSICAL_ROW_DATA_TYPE);
    }

    @Test
    void testlistReadableMetadata() {
        HttpLookupTableSource tableSource =
                (HttpLookupTableSource) createTableSource(SCHEMA, getOptions());
        Map<String, DataType> listMetadataMap = tableSource.listReadableMetadata();
        Map<String, DataType> expectedMap = new LinkedHashMap<>();
        expectedMap.put(HTTP_STATUS_CODE.key, new AtomicDataType(new IntType(true)));
        expectedMap.put(HTTP_HEADERS.key, DataTypes.MAP(DataTypes.STRING(), DataTypes.ARRAY(DataTypes.STRING())));
        expectedMap.put(ERROR_STRING.key, DataTypes.STRING());
        expectedMap.put(HTTP_COMPLETION_STATE.key, DataTypes.STRING());

        assertThat(listMetadataMap).isEqualTo(expectedMap);
    }

    @Test
    void testsummaryString() {
        HttpLookupTableSource tableSource =
                (HttpLookupTableSource) createTableSource(SCHEMA, getOptions());
        assertThat(tableSource.asSummaryString()).isEqualTo("Http Lookup Table Source");
    }

    @Test
    void testreadReadableMetadata() {
        HttpLookupTableSource tableSource =
                (HttpLookupTableSource) createTableSource(SCHEMA, getOptions());
        final String testErrorString = "ABC";
        final int testStatusCode = 500;
        final HttpCompletionState testCompletionState = HttpCompletionState.HTTP_ERROR_STATUS;
        Map<String, List<String>> testHeaders = new HashMap<>();
        testHeaders.put("AAA",List.of("BBB","CCC"));
        testHeaders.put("DDD",List.of("EEE"));

        HttpRowDataWrapper httpRowDataWrapper= new HttpRowDataWrapper(
                                null,
                testErrorString,
                testHeaders,
                500,
                testCompletionState
        );

        assertThat(ERROR_STRING.converter.read(httpRowDataWrapper))
            .isEqualTo(StringData.fromString(testErrorString));
        assertThat(ERROR_STRING.converter.read(null))
                .isNull();
        assertThat(HTTP_STATUS_CODE.converter.read(httpRowDataWrapper))
            .isEqualTo(Integer.valueOf(testStatusCode));
        assertThat(HTTP_STATUS_CODE.converter.read( null))
                .isNull();
        Object readResultForHeaders = HTTP_HEADERS.converter.read(httpRowDataWrapper);
        assertThat(HTTP_HEADERS.converter.read( null))
                .isNull();
        assertThat(readResultForHeaders).isInstanceOf(GenericMapData.class);
        GenericMapData mapData = (GenericMapData) readResultForHeaders;

        // Verify the map has the expected keys
        ArrayData keys = mapData.keyArray();
        assertThat(keys.size()).isEqualTo(2);

        // Create a map to store the converted data for comparison
        Map<String, List<String>> actualMap = new HashMap<>();
        ArrayData valueArray = mapData.valueArray();
        // Extract and convert each key-value pair
        for (int i = 0; i < keys.size(); i++) {
            ArrayData values = valueArray.getArray(i);
            StringData key = keys.getString(i);
            String keyStr = key.toString();
            List<String> valueList = new ArrayList<>();

            // Extract each string from the array
            for (int j = 0; j < values.size(); j++) {
                StringData element = values.getString(j);
                valueList.add(element.toString());
            }

            actualMap.put(keyStr, valueList);
        }
        // Now compare the extracted map with the expected map
        assertThat(actualMap).isEqualTo(testHeaders);

        assertThat(HTTP_COMPLETION_STATE.converter.read(null)).isNull();

        assertThat(HTTP_COMPLETION_STATE.converter.read( httpRowDataWrapper))
                .isEqualTo(StringData.fromString(testCompletionState.name()));
    }

    @Test
    @SuppressWarnings("unchecked")
    void shouldCreateTableSourceWithParams() {
        HttpLookupTableSource tableSource =
                (HttpLookupTableSource) createTableSource(SCHEMA, getOptions());

        LookupTableSource.LookupRuntimeProvider lookupProvider =
                tableSource.getLookupRuntimeProvider(new LookupRuntimeProviderContext(lookupKey));
        HttpTableLookupFunction tableFunction = (HttpTableLookupFunction)
                ((LookupFunctionProvider) lookupProvider).createLookupFunction();

        LookupRow actualLookupRow = tableFunction.getLookupRow();
        assertThat(actualLookupRow).isNotNull();
        assertThat(actualLookupRow.getLookupEntries()).isNotEmpty();
        assertThat(actualLookupRow.getLookupPhysicalRowDataType())
                .isEqualTo(PHYSICAL_ROW_DATA_TYPE);

        HttpLookupConfig actualLookupConfig = tableFunction.getOptions();
        assertThat(actualLookupConfig).isNotNull();
        assertThat(
                actualLookupConfig.getReadableConfig().get(
                        ConfigOptions.key("connector").stringType().noDefaultValue())
        )
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
                actualLookupConfig.getReadableConfig().get(HttpLookupConnectorOptions.ASYNC_POLLING)
        )
                .withFailMessage(
                        "Readable config probably was not passed" +
                                " from Table Factory or it is empty.")
                .isTrue();
    }

    @ParameterizedTest
    @MethodSource("configProvider")
    void testGetLookupRuntimeProvider(TestSpec testSpec) {
        LookupCache cache = new LookupCache() {
            @Override
            public void open(CacheMetricGroup cacheMetricGroup) {

            }

            @Nullable
            @Override
            public Collection<RowData> getIfPresent(RowData rowData) {
                return null;
            }

            @Override
            public Collection<RowData> put(RowData rowData, Collection<RowData> collection) {
                return null;
            }

            @Override
            public void invalidate(RowData rowData) {

            }

            @Override
            public long size() {
                return 0;
            }

            @Override
            public void close() throws Exception {

            }
        };

        HttpLookupConfig options = HttpLookupConfig.builder()
                .useAsync(testSpec.isAsync)
                .build();
        LookupTableSource.LookupRuntimeProvider lookupRuntimeProvider =
                getLookupRuntimeProvider(testSpec.hasCache ? cache : null, options);
        assertTrue(testSpec.expected.isInstance(lookupRuntimeProvider));

    }

    private static class TestSpec {

        boolean hasCache;
        boolean isAsync;

        Class expected;

        private TestSpec(boolean hasCache,
                         boolean isAsync,
                         Class expected) {
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
        return ImmutableList.<TestSpec>builder()
                .addAll(getTestSpecs())
                .build();
    }

    @NotNull
    private static ImmutableList<TestSpec> getTestSpecs() {
        return ImmutableList.of(
                new TestSpec(false, false, LookupFunctionProvider.class),
                new TestSpec(true, false, PartialCachingLookupProvider.class),
                new TestSpec(false, true, AsyncLookupFunctionProvider.class),
                new TestSpec(true, true, PartialCachingAsyncLookupProvider.class)
        );
    }

    private static LookupTableSource.LookupRuntimeProvider
        getLookupRuntimeProvider(LookupCache cache, HttpLookupConfig options) {
        HttpLookupTableSource tableSource = new HttpLookupTableSource(
                null, options,
                null, null, cache);
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
                "url", "http://localhost:8080/service",
                "format", "json");
    }

    class MockHttpResponse implements HttpResponse<byte[]> {
        int statusCode = 0;
        private Map<String, List<String>> headersMap = new HashMap<>();

        @Override
        public int statusCode() {
            return statusCode;
        }

        @Override
        public HttpRequest request() {
            return null;
        }

        @Override
        public Optional<HttpResponse<byte[]>> previousResponse() {
            return Optional.empty();
        }

        @Override
        public HttpHeaders headers() {
            return null;
        }

        @Override
        public URI uri() {
            return null;
        }

        @Override
        public HttpClient.Version version() {
            return null;
        }

        //public void setHeaders(HttpHeaders headers) {
        //    this.headers = headers;
        //}

        public void setStatusCode(int statusCode) {
            this.statusCode = statusCode;
        }

        @Override
        public byte[] body() {
            return new byte[0];
        }

        @Override
        public Optional<SSLSession> sslSession() {
            return Optional.empty();
        }
    }
}
