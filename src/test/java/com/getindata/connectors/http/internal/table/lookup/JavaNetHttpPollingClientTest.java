package com.getindata.connectors.http.internal.table.lookup;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpHeaders;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.*;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.formats.json.JsonFormatFactory;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.ConfigurationException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.getindata.connectors.http.internal.HeaderPreprocessor;
import com.getindata.connectors.http.internal.config.HttpConnectorConfigConstants;
import com.getindata.connectors.http.internal.table.lookup.querycreators.GenericGetQueryCreator;
import com.getindata.connectors.http.internal.table.lookup.querycreators.GenericJsonQueryCreator;
import com.getindata.connectors.http.internal.utils.HttpHeaderUtils;
import static com.getindata.connectors.http.TestHelper.assertPropertyArray;
import static com.getindata.connectors.http.internal.table.lookup.HttpLookupConnectorOptions.SOURCE_LOOKUP_HTTP_IGNORED_RESPONSE_CODES;
import static com.getindata.connectors.http.internal.table.lookup.HttpLookupTableSourceFactory.row;

@ExtendWith(MockitoExtension.class)
public class JavaNetHttpPollingClientTest {

    @Mock
    private HttpClient httpClient;

    @Mock
    private DeserializationSchema<RowData> decoder;

    @Mock
    private LookupRow lookupRow;

    @Mock
    private DynamicTableFactory.Context dynamicTableFactoryContext;
    private HeaderPreprocessor headerPreprocessor;

    private HttpLookupConfig options;

    private static final String BASE_URL = "http://localhost.com";

    @BeforeEach
    public void setUp() {
        this.headerPreprocessor = HttpHeaderUtils.createBasicAuthorizationHeaderPreprocessor();
        this.options = HttpLookupConfig.builder().url(BASE_URL).build();
    }

    @Test
    public void shouldBuildClientWithoutHeaders() throws ConfigurationException {

        JavaNetHttpPollingClient client = new JavaNetHttpPollingClient(
            httpClient,
            decoder,
            options,
            new GetRequestFactory(
                new GenericGetQueryCreator(lookupRow),
                headerPreprocessor,
                options
            )
        );

        assertThat(
            ((GetRequestFactory) client.getRequestFactory()).getHeadersAndValues())
            .isEmpty();
    }

    @Test
    public void shouldBuildGetClientUri() throws ConfigurationException {
        // GIVEN
        JavaNetHttpPollingClient client = new JavaNetHttpPollingClient(
                httpClient,
                decoder,
                options,
                new GetRequestFactory(
                        new GenericGetQueryCreator(lookupRow),
                        headerPreprocessor,
                        options
                )
        );

        DataType lookupPhysicalDataType = row(List.of(
                        DataTypes.FIELD("id", DataTypes.STRING()),
                        DataTypes.FIELD("uuid", DataTypes.STRING())
                ));

        RowData lookupRowData = GenericRowData.of(
                StringData.fromString("1"),
                StringData.fromString("2")
        );

        LookupRow lookupRow = new LookupRow()
                .addLookupEntry(
                        new RowDataSingleValueLookupSchemaEntry("id",
                                RowData.createFieldGetter(
                                        DataTypes.STRING().getLogicalType(),
                                        0)))
                .addLookupEntry(
                        new RowDataSingleValueLookupSchemaEntry("uuid",
                                RowData.createFieldGetter(
                                        DataTypes.STRING().getLogicalType(),
                                        1))
                );
        lookupRow.setLookupPhysicalRowDataType(lookupPhysicalDataType);

        GenericGetQueryCreator queryCreator = new GenericGetQueryCreator(lookupRow);
        LookupQueryInfo lookupQueryInfo = queryCreator.createLookupQuery(lookupRowData);

        // WHEN
        URI uri = ((GetRequestFactory) client.getRequestFactory()).constructGetUri(lookupQueryInfo);

        // THEN
        assertThat(uri.toString()).isEqualTo(BASE_URL + "?id=1&uuid=2");
    }

    @Test
    public void shouldBuildBodyBasedClientUri() {
        // GIVEN
        DataType lookupPhysicalDataType = row(List.of(
                DataTypes.FIELD("id", DataTypes.STRING()),
                DataTypes.FIELD("uuid", DataTypes.STRING())
        ));

        LookupRow lookupRow = new LookupRow()
                .addLookupEntry(
                        new RowDataSingleValueLookupSchemaEntry("id",
                                RowData.createFieldGetter(
                                        DataTypes.STRING().getLogicalType(),
                                        0)))
                .addLookupEntry(
                        new RowDataSingleValueLookupSchemaEntry("uuid",
                                RowData.createFieldGetter(
                                        DataTypes.STRING().getLogicalType(),
                                        1))
                );

        lookupRow.setLookupPhysicalRowDataType(lookupPhysicalDataType);

        SerializationSchema<RowData> jsonSerializer =
                new JsonFormatFactory()
                        .createEncodingFormat(dynamicTableFactoryContext, new Configuration())
                        .createRuntimeEncoder(null, lookupPhysicalDataType);

        BodyBasedRequestFactory requestFactory = new BodyBasedRequestFactory(
                "POST",
                new GenericJsonQueryCreator(jsonSerializer),
                HttpHeaderUtils.createBasicAuthorizationHeaderPreprocessor(),
                HttpLookupConfig.builder()
                        .url(BASE_URL)
                        .build()
        );

        Map<String, String> urlBodyBasedQueryParameters = new LinkedHashMap<>();
        urlBodyBasedQueryParameters.put("key1", "value1");
        urlBodyBasedQueryParameters.put("key2", "value2");

        LookupQueryInfo lookupQueryInfo = new LookupQueryInfo("{}",
                urlBodyBasedQueryParameters, null);

        // WHEN
        HttpRequest httpRequest = requestFactory.setUpRequestMethod(lookupQueryInfo).build();

        // THEN
        assertThat(httpRequest.uri().toString()).isEqualTo(BASE_URL + "?key1=value1&key2=value2");
    }

    @Test
    public void shouldBuildClientWithHeaders() throws ConfigurationException {

        // GIVEN
        Properties properties = new Properties();
        properties.setProperty("property", "val1");
        properties.setProperty("my.property", "val2");
        properties.setProperty(
            HttpConnectorConfigConstants.LOOKUP_SOURCE_HEADER_PREFIX + "Origin",
            "https://developer.mozilla.org");

        properties.setProperty(
            HttpConnectorConfigConstants.LOOKUP_SOURCE_HEADER_PREFIX + "Cache-Control",
            "no-cache, no-store, max-age=0, must-revalidate"
        );
        properties.setProperty(
            HttpConnectorConfigConstants.LOOKUP_SOURCE_HEADER_PREFIX
                + "Access-Control-Allow-Origin", "*"
        );

        // WHEN
        HttpLookupConfig lookupConfig = HttpLookupConfig.builder()
            .properties(properties)
            .build();

        JavaNetHttpPollingClient client = new JavaNetHttpPollingClient(
            httpClient,
            decoder,
            lookupConfig,
            new GetRequestFactory(
                new GenericGetQueryCreator(lookupRow),
                headerPreprocessor,
                lookupConfig
            )
        );

        String[] headersAndValues =
            ((GetRequestFactory) client.getRequestFactory()).getHeadersAndValues();
        assertThat(headersAndValues).hasSize(6);

        // THEN
        // assert that we have property followed by its value.
        assertPropertyArray(headersAndValues, "Origin", "https://developer.mozilla.org");
        assertPropertyArray(
            headersAndValues,
            "Cache-Control", "no-cache, no-store, max-age=0, must-revalidate"
        );
        assertPropertyArray(headersAndValues, "Access-Control-Allow-Origin", "*");
    }

    @Test
    public void shouldSetIgnoreStatusCodeCompletionStateForIgnoredStatusCodes() throws Exception {
        // GIVEN - Configure client with ignored status codes (404, 503)
        Configuration config = new Configuration();
        config.setString(SOURCE_LOOKUP_HTTP_IGNORED_RESPONSE_CODES.key(), "404,503");
        // Set success codes to 200 to avoid conflicts
        config.setString(HttpLookupConnectorOptions.SOURCE_LOOKUP_HTTP_SUCCESS_CODES.key(), "200");
        // Set retry codes to empty or different codes to avoid conflicts
        config.setString(HttpLookupConnectorOptions.SOURCE_LOOKUP_HTTP_RETRY_CODES.key(), "");

        Properties properties = new Properties();
        HttpLookupConfig lookupConfig = HttpLookupConfig.builder()
            .url(BASE_URL)
            .readableConfig(config)
            .properties(properties)
            .httpPostRequestCallback(new Slf4JHttpLookupPostRequestCallback())
            .build();

        // Mock HTTP response with status code 404 (ignored status)
        @SuppressWarnings("unchecked")
        HttpResponse<String> mockResponse = (HttpResponse<String>) mock(HttpResponse.class);
        when(mockResponse.statusCode()).thenReturn(404);
        when(mockResponse.body()).thenReturn("Not Found");
        lenient().when(mockResponse.headers()).thenReturn(HttpHeaders.of(
            Collections.emptyMap(),
            (name, value) -> true
        ));

        // Mock HttpClient to return the mocked response
        HttpClient mockHttpClient = mock(HttpClient.class);
        when(mockHttpClient.send(any(), any())).thenReturn((HttpResponse) mockResponse);

        DataType lookupPhysicalDataType = row(List.of(
            DataTypes.FIELD("id", DataTypes.STRING())
        ));

        LookupRow lookupRow = new LookupRow()
            .addLookupEntry(
                new RowDataSingleValueLookupSchemaEntry("id",
                    RowData.createFieldGetter(
                        DataTypes.STRING().getLogicalType(),
                        0))
            );
        lookupRow.setLookupPhysicalRowDataType(lookupPhysicalDataType);

        JavaNetHttpPollingClient client = new JavaNetHttpPollingClient(
            mockHttpClient,
            decoder,
            lookupConfig,
            new GetRequestFactory(
                new GenericGetQueryCreator(lookupRow),
                headerPreprocessor,
                lookupConfig
            )
        );

        RowData lookupRowData = GenericRowData.of(StringData.fromString("1"));

        // WHEN - Pull data with a lookup row
        HttpRowDataWrapper result = client.pull(lookupRowData);

        // THEN - Verify completion state is IGNORE_STATUS_CODE
        assertThat(result.getHttpCompletionState())
            .isEqualTo(HttpCompletionState.IGNORE_STATUS_CODE);
        assertThat(result.getData()).isEmpty();
    }

    @Test
    public void shouldSetIgnoreStatusCodeForMultipleIgnoredCodes() throws Exception {
        // GIVEN - Configure client with multiple ignored status codes
        Configuration config = new Configuration();
        config.setString(SOURCE_LOOKUP_HTTP_IGNORED_RESPONSE_CODES.key(), "404,503,429");
        // Set success codes to 200 to avoid conflicts
        config.setString(HttpLookupConnectorOptions.SOURCE_LOOKUP_HTTP_SUCCESS_CODES.key(), "200");
        // Set retry codes to empty or different codes to avoid conflicts
        config.setString(HttpLookupConnectorOptions.SOURCE_LOOKUP_HTTP_RETRY_CODES.key(), "");

        Properties properties = new Properties();
        HttpLookupConfig lookupConfig = HttpLookupConfig.builder()
            .url(BASE_URL)
            .readableConfig(config)
            .properties(properties)
            .httpPostRequestCallback(new Slf4JHttpLookupPostRequestCallback())
            .build();

        // Test with status code 503
        @SuppressWarnings("unchecked")
        HttpResponse<String> mockResponse = (HttpResponse<String>) mock(HttpResponse.class);
        when(mockResponse.statusCode()).thenReturn(503);
        when(mockResponse.body()).thenReturn("Service Unavailable");
        lenient().when(mockResponse.headers()).thenReturn(HttpHeaders.of(
            Collections.emptyMap(),
            (name, value) -> true
        ));

        HttpClient mockHttpClient = mock(HttpClient.class);
        when(mockHttpClient.send(any(), any())).thenReturn((HttpResponse) mockResponse);

        DataType lookupPhysicalDataType = row(List.of(
            DataTypes.FIELD("id", DataTypes.STRING())
        ));

        LookupRow lookupRow = new LookupRow()
            .addLookupEntry(
                new RowDataSingleValueLookupSchemaEntry("id",
                    RowData.createFieldGetter(
                        DataTypes.STRING().getLogicalType(),
                        0))
            );
        lookupRow.setLookupPhysicalRowDataType(lookupPhysicalDataType);

        JavaNetHttpPollingClient client = new JavaNetHttpPollingClient(
            mockHttpClient,
            decoder,
            lookupConfig,
            new GetRequestFactory(
                new GenericGetQueryCreator(lookupRow),
                headerPreprocessor,
                lookupConfig
            )
        );

        RowData lookupRowData = GenericRowData.of(StringData.fromString("1"));

        // WHEN
        HttpRowDataWrapper result = client.pull(lookupRowData);

        // THEN - Verify 503 is also treated as ignored
        assertThat(result.getHttpCompletionState())
            .isEqualTo(HttpCompletionState.IGNORE_STATUS_CODE);
        assertThat(result.getData()).isEmpty();
    }

    @Test
    public void shouldNotSetIgnoreStatusCodeForNonIgnoredCodes() throws Exception {
        // GIVEN - Configure client with ignored status codes (404, 503)
        Configuration config = new Configuration();
        config.setString(SOURCE_LOOKUP_HTTP_IGNORED_RESPONSE_CODES.key(), "404,503");
        // Set success codes to 200 to avoid conflicts
        config.setString(HttpLookupConnectorOptions.SOURCE_LOOKUP_HTTP_SUCCESS_CODES.key(), "200");
        // Set retry codes to empty or different codes to avoid conflicts
        config.setString(HttpLookupConnectorOptions.SOURCE_LOOKUP_HTTP_RETRY_CODES.key(), "");

        Properties properties = new Properties();
        HttpLookupConfig lookupConfig = HttpLookupConfig.builder()
            .url(BASE_URL)
            .readableConfig(config)
            .properties(properties)
            .httpPostRequestCallback(new Slf4JHttpLookupPostRequestCallback())
            .build();

        // Mock HTTP response with status code 200 (success, not ignored)
        @SuppressWarnings("unchecked")
        HttpResponse<String> mockResponse = (HttpResponse<String>) mock(HttpResponse.class);
        when(mockResponse.statusCode()).thenReturn(200);
        when(mockResponse.body()).thenReturn("{\"id\":\"1\",\"name\":\"test\"}");
        lenient().when(mockResponse.headers()).thenReturn(HttpHeaders.of(
            Collections.emptyMap(),
            (name, value) -> true
        ));

        HttpClient mockHttpClient = mock(HttpClient.class);
        when(mockHttpClient.send(any(), any())).thenReturn((HttpResponse) mockResponse);

        // Mock decoder to return a row
        RowData mockRowData = GenericRowData.of(
            StringData.fromString("1"),
            StringData.fromString("test")
        );
        when(decoder.deserialize(any(byte[].class))).thenReturn(mockRowData);

        DataType lookupPhysicalDataType = row(List.of(
            DataTypes.FIELD("id", DataTypes.STRING())
        ));

        LookupRow lookupRow = new LookupRow()
            .addLookupEntry(
                new RowDataSingleValueLookupSchemaEntry("id",
                    RowData.createFieldGetter(
                        DataTypes.STRING().getLogicalType(),
                        0))
            );
        lookupRow.setLookupPhysicalRowDataType(lookupPhysicalDataType);

        JavaNetHttpPollingClient client = new JavaNetHttpPollingClient(
            mockHttpClient,
            decoder,
            lookupConfig,
            new GetRequestFactory(
                new GenericGetQueryCreator(lookupRow),
                headerPreprocessor,
                lookupConfig
            )
        );

        RowData lookupRowData = GenericRowData.of(StringData.fromString("1"));

        // WHEN
        HttpRowDataWrapper result = client.pull(lookupRowData);

        // THEN - Verify completion state is SUCCESS, not IGNORE_STATUS_CODE
        assertThat(result.getHttpCompletionState())
            .isEqualTo(HttpCompletionState.SUCCESS);
        assertThat(result.getData()).isNotEmpty();
    }

    @Test
    public void shouldReturnMetadataForIgnoredStatusCode() throws Exception {
        // GIVEN - Configure client with ignored status codes (404)
        Configuration config = new Configuration();
        config.setString(SOURCE_LOOKUP_HTTP_IGNORED_RESPONSE_CODES.key(), "404");
        // Set success codes to 200 to avoid conflicts
        config.setString(HttpLookupConnectorOptions.SOURCE_LOOKUP_HTTP_SUCCESS_CODES.key(), "200");
        // Set retry codes to empty or different codes to avoid conflicts
        config.setString(HttpLookupConnectorOptions.SOURCE_LOOKUP_HTTP_RETRY_CODES.key(), "");

        Properties properties = new Properties();
        HttpLookupConfig lookupConfig = HttpLookupConfig.builder()
            .url(BASE_URL)
            .readableConfig(config)
            .properties(properties)
            .httpPostRequestCallback(new Slf4JHttpLookupPostRequestCallback())
            .build();

        // Mock HTTP response with status code 404 (ignored status) and metadata
        @SuppressWarnings("unchecked")
        HttpResponse<String> mockResponse = (HttpResponse<String>) mock(HttpResponse.class);
        when(mockResponse.statusCode()).thenReturn(404);
        when(mockResponse.body()).thenReturn("Not Found");

        // Add metadata headers
        Map<String, List<String>> headersMap = new HashMap<>();
        headersMap.put("X-Request-Id", List.of("12345"));
        headersMap.put("X-Custom-Header", List.of("custom-value"));
        when(mockResponse.headers()).thenReturn(HttpHeaders.of(
            headersMap,
            (name, value) -> true
        ));

        // Mock HttpClient to return the mocked response
        HttpClient mockHttpClient = mock(HttpClient.class);
        when(mockHttpClient.send(any(), any())).thenReturn((HttpResponse) mockResponse);

        DataType lookupPhysicalDataType = row(List.of(
            DataTypes.FIELD("id", DataTypes.STRING())
        ));

        LookupRow lookupRow = new LookupRow()
            .addLookupEntry(
                new RowDataSingleValueLookupSchemaEntry("id",
                    RowData.createFieldGetter(
                        DataTypes.STRING().getLogicalType(),
                        0))
            );
        lookupRow.setLookupPhysicalRowDataType(lookupPhysicalDataType);

        JavaNetHttpPollingClient client = new JavaNetHttpPollingClient(
            mockHttpClient,
            decoder,
            lookupConfig,
            new GetRequestFactory(
                new GenericGetQueryCreator(lookupRow),
                headerPreprocessor,
                lookupConfig
            )
        );

        RowData lookupRowData = GenericRowData.of(StringData.fromString("1"));

        // WHEN - Pull data with a lookup row
        HttpRowDataWrapper result = client.pull(lookupRowData);

        // THEN - Verify completion state is IGNORE_STATUS_CODE
        assertThat(result.getHttpCompletionState())
            .isEqualTo(HttpCompletionState.IGNORE_STATUS_CODE);
        // Verify data is empty (no body content)
        assertThat(result.getData()).isEmpty();
        // Verify metadata is present - status code
        assertThat(result.getHttpStatusCode()).isEqualTo(404);
        // Verify metadata is present - headers
        assertThat(result.getHttpHeadersMap()).isNotNull();
        assertThat(result.getHttpHeadersMap()).containsKey("X-Request-Id");
        assertThat(result.getHttpHeadersMap().get("X-Request-Id")).containsExactly("12345");
        assertThat(result.getHttpHeadersMap()).containsKey("X-Custom-Header");
        assertThat(result.getHttpHeadersMap().get("X-Custom-Header")).containsExactly("custom-value");
    }
}
