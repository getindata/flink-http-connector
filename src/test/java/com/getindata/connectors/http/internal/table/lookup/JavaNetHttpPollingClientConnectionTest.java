package com.getindata.connectors.http.internal.table.lookup;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.stream.Stream;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.client.MappingBuilder;
import com.github.tomakehurst.wiremock.matching.RequestPatternBuilder;
import com.github.tomakehurst.wiremock.stubbing.StubMapping;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.formats.json.JsonFormatFactory;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.factories.DynamicTableFactory.Context;
import org.apache.flink.table.runtime.connector.source.LookupRuntimeProviderContext;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.ConfigurationException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.equalToJson;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.put;
import static com.github.tomakehurst.wiremock.client.WireMock.putRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

import com.getindata.connectors.http.internal.config.HttpConnectorConfigConstants;
import com.getindata.connectors.http.internal.table.lookup.querycreators.GenericGetQueryCreator;
import com.getindata.connectors.http.internal.table.lookup.querycreators.GenericJsonQueryCreator;
import com.getindata.connectors.http.internal.utils.HttpHeaderUtils;
import com.getindata.connectors.http.internal.utils.SerializationSchemaUtils;
import static com.getindata.connectors.http.TestHelper.readTestFile;
import static com.getindata.connectors.http.internal.config.HttpConnectorConfigConstants.RESULT_TYPE;
import static com.getindata.connectors.http.internal.table.lookup.HttpLookupConnectorOptions.SOURCE_LOOKUP_HTTP_IGNORED_RESPONSE_CODES;
import static com.getindata.connectors.http.internal.table.lookup.HttpLookupConnectorOptions.SOURCE_LOOKUP_HTTP_SUCCESS_CODES;
import static com.getindata.connectors.http.internal.table.lookup.HttpLookupTableSourceFactory.row;

@ExtendWith(MockitoExtension.class)
class JavaNetHttpPollingClientConnectionTest {

    private static final String SAMPLES_FOLDER = "/http/";
    private static final String SAMPLES_FOLDER_ARRAY_RESULT = "/http-array-result/";
    private static final String SAMPLES_FOLDER_ARRAY_RESULT_WITH_NULLS =
            "/http-array-result-with-nulls/";

    private static final String ENDPOINT = "/service";

    private static WireMockServer wireMockServer;

    @Mock
    private Context dynamicTableFactoryContext;

    private DynamicTableSource.Context dynamicTableSourceContext;

    private StubMapping stubMapping;

    private Properties properties;

    private Configuration configuration;

    private RowData lookupRowData;

    private DataType lookupPhysicalDataType;

    @BeforeAll
    static void setUpAll() {
        wireMockServer = new WireMockServer();
        wireMockServer.start();
    }

    @AfterAll
    static void cleanUpAll() {
        if (wireMockServer != null) {
            wireMockServer.stop();
        }
    }

    @BeforeEach
    void setUp() {
        int[][] lookupKey = {{}};
        this.dynamicTableSourceContext = new LookupRuntimeProviderContext(lookupKey);

        this.lookupRowData = GenericRowData.of(
                StringData.fromString("1"),
                StringData.fromString("2")
        );

        this.lookupPhysicalDataType = row(List.of(
                        DataTypes.FIELD("id", DataTypes.STRING()),
                        DataTypes.FIELD("uuid", DataTypes.STRING())
                )
        );

        this.properties = new Properties();
        this.properties.setProperty(
                HttpConnectorConfigConstants.LOOKUP_SOURCE_HEADER_PREFIX + "Content-Type",
                "application/json"
        );
        this.properties.setProperty(RESULT_TYPE, "single-value");

        this.configuration = new Configuration();
    }

    @AfterEach
    void cleanUp() {
        if (stubMapping != null && wireMockServer != null) {
            wireMockServer.removeStub(stubMapping);
        }
    }

    @Test
    void shouldQuery200WithParams() throws ConfigurationException {

        // GIVEN
        this.stubMapping = setUpServerStub(200);
        JavaNetHttpPollingClient pollingClient = setUpPollingClient();

        // WHEN
        Collection<RowData> results = pollingClient.pull(lookupRowData);

        // THEN
        wireMockServer.verify(RequestPatternBuilder.forCustomMatcher(stubMapping.getRequest()));

        assertThat(results).hasSize(1);
        RowData result = results.iterator().next();
        assertThat(result.getArity()).isEqualTo(4);
        assertThat(result.getString(1)
                .toString()).isEqualTo("Returned HTTP message for parameter PARAM, COUNTER");

        RowData detailsRow = result.getRow(3, 2);
        assertThat(detailsRow.getBoolean(0)).isEqualTo(true);

        RowData nestedDetailsRow = detailsRow.getRow(1, 1);
        assertThat(nestedDetailsRow.getString(0).toString()).isEqualTo("$1,729.34");
    }

    @ParameterizedTest
    @ValueSource(strings = {"PUT", "POST"})
    void shouldQuery200WithBodyParams(String methodName) throws ConfigurationException {

        // GIVEN
        this.stubMapping = setUpServerBodyStub(methodName);
        JavaNetHttpPollingClient pollingClient = setUpPollingClient(setUpBodyRequestFactory(methodName));

        // WHEN
        Collection<RowData> results = pollingClient.pull(lookupRowData);

        // THEN
        wireMockServer.verify(RequestPatternBuilder.forCustomMatcher(stubMapping.getRequest()));
        if (methodName.equalsIgnoreCase("POST")) {
            wireMockServer.verify(postRequestedFor(urlEqualTo(ENDPOINT)));
        } else if (methodName.equalsIgnoreCase("PUT")) {
            wireMockServer.verify(putRequestedFor(urlEqualTo(ENDPOINT)));
        } else {
            fail("Unexpected REST method.");
        }

        assertThat(results).hasSize(1);
        RowData result = results.iterator().next();
        assertThat(result.getArity()).isEqualTo(4);
        assertThat(result.getString(1)
                .toString()).isEqualTo("Returned HTTP message for parameter PARAM, COUNTER");

        RowData detailsRow = result.getRow(3, 2);
        assertThat(detailsRow.getBoolean(0)).isEqualTo(true);

        RowData nestedDetailsRow = detailsRow.getRow(1, 1);
        assertThat(nestedDetailsRow.getString(0).toString()).isEqualTo("$1,729.34");
    }

    private static Stream<Arguments> clientErrorCodeConfig() {
        return Stream.of(
                Arguments.of("2XX", "", false),
                Arguments.of("2XX", "201", true),
                Arguments.of("200,201,202", "202", false),
                Arguments.of("200,201", "202", false)
        );
    }

    @Test
    void shouldQuery200WithArrayResult() throws ConfigurationException {
        // GIVEN
        this.stubMapping = setUpServerStubArrayResult(200);
        properties.setProperty(RESULT_TYPE, "array");

        // WHEN
        JavaNetHttpPollingClient pollingClient = setUpPollingClient();

        // WHEN
        Collection<RowData> results = pollingClient.pull(lookupRowData);

        // THEN
        wireMockServer.verify(RequestPatternBuilder.forCustomMatcher(stubMapping.getRequest()));

        assertThat(results).hasSize(2);

        Iterator<RowData> iterator = results.iterator();

        RowData firstResult = iterator.next();
        assertThat(firstResult.getArity()).isEqualTo(4);
        RowData detailsRow1 = firstResult.getRow(3, 2);
        assertThat(detailsRow1.getBoolean(0)).isEqualTo(true); // isActive
        RowData nestedDetailsRow1 = detailsRow1.getRow(1, 1);
        assertThat(nestedDetailsRow1.getString(0).toString()).isEqualTo("$1,729.34");

        RowData secondResult = iterator.next();
        assertThat(secondResult.getArity()).isEqualTo(4);
        RowData detailsRow2 = secondResult.getRow(3, 2);
        assertThat(detailsRow2.getBoolean(0)).isEqualTo(false); // isActive
        RowData nestedDetailsRow2 = detailsRow2.getRow(1, 1);
        assertThat(nestedDetailsRow2.getString(0).toString()).isEqualTo("$22,001.99");
    }

    @Test
    void shouldQuery200WithArrayResultWithNulls() throws ConfigurationException {
        // GIVEN
        this.stubMapping = setUpServerStubArrayResultWithNulls(200);
        properties.setProperty(RESULT_TYPE, "array");

        // WHEN
        JavaNetHttpPollingClient pollingClient = setUpPollingClient();

        // WHEN
        Collection<RowData> results = pollingClient.pull(lookupRowData);

        // THEN
        wireMockServer.verify(RequestPatternBuilder.forCustomMatcher(stubMapping.getRequest()));

        assertThat(results).hasSize(1);

        Iterator<RowData> iterator = results.iterator();

        RowData firstResult = iterator.next();
        assertThat(firstResult.getArity()).isEqualTo(4);
        RowData detailsRow1 = firstResult.getRow(3, 2);
        assertThat(detailsRow1.getBoolean(0)).isEqualTo(true); // isActive
        RowData nestedDetailsRow1 = detailsRow1.getRow(1, 1);
        assertThat(nestedDetailsRow1.getString(0).toString()).isEqualTo("$1,729.34");
    }

    @ParameterizedTest
    @MethodSource("clientErrorCodeConfig")
    void shouldHandleCodeBasedOnConfiguration(
            String successCodesExpression,
            String ignoredResponseCodesExpression,
            boolean isExpectedResponseEmpty
    ) throws ConfigurationException {

        // GIVEN
        this.stubMapping = setUpServerStub(201);
        configuration.setString(SOURCE_LOOKUP_HTTP_SUCCESS_CODES, successCodesExpression);
        configuration.setString(SOURCE_LOOKUP_HTTP_IGNORED_RESPONSE_CODES, ignoredResponseCodesExpression);
        JavaNetHttpPollingClient pollingClient = setUpPollingClient();

        // WHEN
        Collection<RowData> results = pollingClient.pull(lookupRowData);

        // THEN
        assertThat(results.isEmpty()).isEqualTo(isExpectedResponseEmpty);
    }

    @Test
    void shouldFailOnServerError() throws ConfigurationException {

        // GIVEN
        this.stubMapping = setUpServerStub(500);
        JavaNetHttpPollingClient pollingClient = setUpPollingClient();

        assertThrows(RuntimeException.class, () -> pollingClient.pull(lookupRowData));
    }

    @Test
    void shouldProcessWithMissingArguments() throws ConfigurationException {

        // GIVEN
        this.stubMapping = setUpServerStub(200);
        JavaNetHttpPollingClient pollingClient = setUpPollingClient();

        // WHEN
        Collection<RowData> results = pollingClient.pull(null);

        // THEN
        assertThat(results.isEmpty()).isTrue();
    }

    @ParameterizedTest
    @CsvSource({
        "user:password, false",
        "Basic dXNlcjpwYXNzd29yZA==, false",
        "Basic dXNlcjpwYXNzd29yZA==, true"
    })
    public void shouldConnectWithBasicAuth(String authorizationHeaderValue,
                                           boolean useRawAuthHeader) throws ConfigurationException {

        // GIVEN
        this.stubMapping = setupServerStubForBasicAuth();

        properties.setProperty(
                HttpConnectorConfigConstants.LOOKUP_SOURCE_HEADER_PREFIX + "Authorization",
                authorizationHeaderValue
        );

        properties.setProperty(
                HttpConnectorConfigConstants.LOOKUP_SOURCE_HEADER_USE_RAW,
                Boolean.toString(useRawAuthHeader)
        );

        JavaNetHttpPollingClient pollingClient = setUpPollingClient();

        // WHEN
        Collection<RowData> results = pollingClient.pull(lookupRowData);

        // THEN
        wireMockServer.verify(RequestPatternBuilder.forCustomMatcher(stubMapping.getRequest()));

        assertThat(results).hasSize(1);
        RowData result = results.iterator().next();
        assertThat(result.getArity()).isEqualTo(4);
        assertThat(result.getString(1)
                .toString()).isEqualTo("Returned HTTP message for parameter PARAM, COUNTER");

        RowData detailsRow = result.getRow(3, 2);
        assertThat(detailsRow.getBoolean(0)).isEqualTo(true);

        RowData nestedDetailsRow = detailsRow.getRow(1, 1);
        assertThat(nestedDetailsRow.getString(0).toString()).isEqualTo("$1,729.34");
    }

    private String getBaseUrl() {
        return wireMockServer.baseUrl() + ENDPOINT;
    }

    public JavaNetHttpPollingClient setUpPollingClient() throws ConfigurationException {
        return setUpPollingClient(setUpGetRequestFactory());
    }

    private GetRequestFactory setUpGetRequestFactory() {
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

        boolean useRawAuthHeader = Boolean.parseBoolean(
                (String) properties.get(HttpConnectorConfigConstants.LOOKUP_SOURCE_HEADER_USE_RAW));

        return new GetRequestFactory(
                new GenericGetQueryCreator(lookupRow),
                HttpHeaderUtils.createBasicAuthorizationHeaderPreprocessor(useRawAuthHeader),
                HttpLookupConfig.builder()
                        .url(getBaseUrl())
                        .readableConfig(configuration)
                        .properties(properties)
                        .build()
        );
    }

    private BodyBasedRequestFactory setUpBodyRequestFactory(String methodName) {

        SerializationSchema<RowData> jsonSerializer =
                new JsonFormatFactory()
                        .createEncodingFormat(dynamicTableFactoryContext, new Configuration())
                        .createRuntimeEncoder(null, lookupPhysicalDataType);

        boolean useRawAuthHeader = Boolean.parseBoolean(
                (String) properties.get(HttpConnectorConfigConstants.LOOKUP_SOURCE_HEADER_USE_RAW));

        return new BodyBasedRequestFactory(
                methodName,
                new GenericJsonQueryCreator(jsonSerializer),
                HttpHeaderUtils.createBasicAuthorizationHeaderPreprocessor(useRawAuthHeader),
                HttpLookupConfig.builder()
                        .url(getBaseUrl())
                        .properties(properties)
                        .build()
        );
    }

    private JavaNetHttpPollingClient setUpPollingClient(
            HttpRequestFactory requestFactory) throws ConfigurationException {

        HttpLookupConfig lookupConfig = HttpLookupConfig.builder()
                .url(getBaseUrl())
                .readableConfig(configuration)
                .properties(properties)
                .httpPostRequestCallback(new Slf4JHttpLookupPostRequestCallback())
                .build();

        DataType physicalDataType = DataTypes.ROW(
                DataTypes.FIELD("id", DataTypes.STRING()),
                DataTypes.FIELD("msg", DataTypes.STRING()),
                DataTypes.FIELD("uuid", DataTypes.STRING()),
                DataTypes.FIELD("details", DataTypes.ROW(
                        DataTypes.FIELD("isActive", DataTypes.BOOLEAN()),
                        DataTypes.FIELD("nestedDetails", DataTypes.ROW(
                                DataTypes.FIELD("balance", DataTypes.STRING())
                        ))
                ))
        );

        DeserializationSchema<RowData> schemaDecoder =
                new JsonFormatFactory()
                        .createDecodingFormat(dynamicTableFactoryContext, new Configuration())
                        .createRuntimeDecoder(dynamicTableSourceContext, physicalDataType);

        try {
            schemaDecoder.open(
                    SerializationSchemaUtils.createDeserializationInitContext(
                            JavaNetHttpPollingClientConnectionTest.class));
        } catch (Exception e) {
            throw new RuntimeException("Unable to open schema decoder: " + e.getMessage(), e);
        }

        JavaNetHttpPollingClientFactory pollingClientFactory =
                new JavaNetHttpPollingClientFactory(requestFactory);

        return pollingClientFactory.createPollClient(lookupConfig, schemaDecoder);
    }

    private StubMapping setUpServerStub(int status) {
        return wireMockServer.stubFor(
                get(urlEqualTo(ENDPOINT + "?id=1&uuid=2"))
                        .withHeader("Content-Type", equalTo("application/json"))
                        .willReturn(
                                aResponse()
                                        .withStatus(status)
                                        .withBody(readTestFile(SAMPLES_FOLDER + "HttpResult.json"))));
    }

    private StubMapping setUpServerBodyStub(String methodName) {
        MappingBuilder methodStub = (methodName.equalsIgnoreCase("PUT") ?
                put(urlEqualTo(ENDPOINT)) :
                post(urlEqualTo(ENDPOINT)));

        return wireMockServer.stubFor(
                methodStub
                        .withHeader("Content-Type", equalTo("application/json"))
                        .withRequestBody(equalToJson("{\"id\" : \"1\", \"uuid\" : \"2\"}"))
                        .willReturn(
                                aResponse()
                                        .withStatus(200)
                                        .withBody(readTestFile(SAMPLES_FOLDER + "HttpResult.json"))));
    }

    private StubMapping setUpServerStubArrayResult(int status) {
        return wireMockServer.stubFor(
                get(urlEqualTo(ENDPOINT + "?id=1&uuid=2"))
                        .withHeader("Content-Type", equalTo("application/json"))
                        .willReturn(
                                aResponse()
                                        .withStatus(status)
                                        .withBody(readTestFile(SAMPLES_FOLDER_ARRAY_RESULT + "HttpResult.json"))));
    }

    private StubMapping setUpServerStubArrayResultWithNulls(int status) {
        return wireMockServer.stubFor(
                get(urlEqualTo(ENDPOINT + "?id=1&uuid=2"))
                        .withHeader("Content-Type", equalTo("application/json"))
                        .willReturn(
                                aResponse()
                                        .withStatus(status)
                                        .withBody(readTestFile(
                                                SAMPLES_FOLDER_ARRAY_RESULT_WITH_NULLS + "HttpResult.json"))));
    }

    private StubMapping setupServerStubForBasicAuth() {
        return wireMockServer.stubFor(get(urlEqualTo(ENDPOINT + "?id=1&uuid=2"))
                .withHeader("Content-Type", equalTo("application/json"))
                .withBasicAuth("user", "password")
                .willReturn(
                        aResponse()
                                .withStatus(200)
                                .withBody(readTestFile(SAMPLES_FOLDER + "HttpResult.json"))));
    }
}
