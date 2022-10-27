package com.getindata.connectors.http.internal.table.lookup;

import java.util.List;
import java.util.Optional;
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
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
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
import static org.junit.jupiter.api.Assertions.fail;

import com.getindata.connectors.http.internal.config.HttpConnectorConfigConstants;
import com.getindata.connectors.http.internal.table.lookup.querycreators.GenericGetQueryCreator;
import com.getindata.connectors.http.internal.table.lookup.querycreators.GenericJsonQueryCreator;
import com.getindata.connectors.http.internal.utils.HttpHeaderUtils;
import static com.getindata.connectors.http.TestHelper.readTestFile;
import static com.getindata.connectors.http.internal.table.lookup.HttpLookupTableSourceFactory.row;

@ExtendWith(MockitoExtension.class)
class JavaNetHttpPollingClientConnectionTest {

    private static final String SAMPLES_FOLDER = "/http/";

    private static final String ENDPOINT = "/service";

    private static WireMockServer wireMockServer;

    @Mock
    private Context dynamicTableFactoryContext;

    private DynamicTableSource.Context dynamicTableSourceContext;

    private StubMapping stubMapping;

    private Properties properties;

    private RowData lookupRowData;

    private DataType lookupPhysicalDataType;

    @BeforeAll
    public static void setUpAll() {
        wireMockServer = new WireMockServer();
        wireMockServer.start();
    }

    @AfterAll
    public static void cleanUpAll() {
        if (wireMockServer != null) {
            wireMockServer.stop();
        }
    }

    @BeforeEach
    public void setUp() {
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
    }

    @AfterEach
    public void cleanUp() {
        if (stubMapping != null && wireMockServer != null) {
            wireMockServer.removeStub(stubMapping);
        }
    }

    @Test
    void shouldQuery200WithParams() {

        // GIVEN
        this.stubMapping = setUpServerStub(200);
        JavaNetHttpPollingClient pollingClient = setUpPollingClient(getBaseUrl());

        // WHEN
        RowData result = pollingClient.pull(lookupRowData).orElseThrow();

        // THEN
        wireMockServer.verify(RequestPatternBuilder.forCustomMatcher(stubMapping.getRequest()));

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
    void shouldQuery200WithBodyParams(String methodName) {

        // GIVEN
        this.stubMapping = setUpServerBodyStub(methodName);
        JavaNetHttpPollingClient pollingClient =
            setUpPollingClient(
                getBaseUrl(),
                properties,
                setUpBodyRequestFactory(methodName, properties)
            );

        // WHEN
        RowData result = pollingClient.pull(lookupRowData).orElseThrow();

        // THEN
        wireMockServer.verify(RequestPatternBuilder.forCustomMatcher(stubMapping.getRequest()));
        if (methodName.equalsIgnoreCase("POST")) {
            wireMockServer.verify(postRequestedFor(urlEqualTo(ENDPOINT)));
        } else if (methodName.equalsIgnoreCase("PUT")) {
            wireMockServer.verify(putRequestedFor(urlEqualTo(ENDPOINT)));
        } else {
            fail("Unexpected REST method.");
        }

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
            Arguments.of(prepareErrorCodeProperties("4XX", ""), false),
            Arguments.of(prepareErrorCodeProperties("2XX", " "), true),
            Arguments.of(prepareErrorCodeProperties("2xx", "201"), false)
        );
    }

    @ParameterizedTest
    @MethodSource("clientErrorCodeConfig")
    void shouldHandleCodeBasedOnConfiguration(
            Properties properties,
            boolean isExpectedResponseEmpty) {

        // GIVEN
        this.stubMapping = setUpServerStub(201);
        JavaNetHttpPollingClient pollingClient = setUpPollingClient(
            getBaseUrl(),
            properties,
            setUpGetRequestFactory(properties)
        );

        // WHEN
        Optional<RowData> poll = pollingClient.pull(lookupRowData);

        // THEN
        assertThat(poll.isEmpty()).isEqualTo(isExpectedResponseEmpty);
    }

    @Test
    void shouldHandleServerError() {

        // GIVEN
        this.stubMapping = setUpServerStub(500);
        JavaNetHttpPollingClient pollingClient = setUpPollingClient(getBaseUrl());

        // WHEN
        Optional<RowData> poll = pollingClient.pull(lookupRowData);

        // THEN
        wireMockServer.verify(RequestPatternBuilder.forCustomMatcher(stubMapping.getRequest()));

        assertThat(poll.isEmpty()).isTrue();
    }

    @Test
    void shouldProcessWithMissingArguments() {

        // GIVEN
        this.stubMapping = setUpServerStub(200);
        JavaNetHttpPollingClient pollingClient = setUpPollingClient(getBaseUrl());

        // WHEN
        Optional<RowData> poll = pollingClient.pull(null);

        // THEN
        assertThat(poll.isEmpty()).isTrue();
    }

    @ParameterizedTest
    @ValueSource(strings = {
        "user:password",
        "Basic dXNlcjpwYXNzd29yZA=="
    })
    public void shouldConnectWithBasicAuth(String authorizationHeaderValue) {

        // GIVEN
        this.stubMapping = setupServerStubForBasicAuth();

        Properties properties = new Properties();
        properties.setProperty(
            HttpConnectorConfigConstants.LOOKUP_SOURCE_HEADER_PREFIX + "Content-Type",
            "application/json"
        );

        properties.setProperty(
            HttpConnectorConfigConstants.LOOKUP_SOURCE_HEADER_PREFIX + "Authorization",
            authorizationHeaderValue
        );

        JavaNetHttpPollingClient pollingClient = setUpPollingClient(
            getBaseUrl(),
            properties,
            setUpGetRequestFactory(properties)
        );

        // WHEN
        RowData result = pollingClient.pull(lookupRowData).orElseThrow();

        // THEN
        wireMockServer.verify(RequestPatternBuilder.forCustomMatcher(stubMapping.getRequest()));

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

    public JavaNetHttpPollingClient setUpPollingClient(String url) {

        Properties properties = new Properties();
        properties.setProperty(
            HttpConnectorConfigConstants.LOOKUP_SOURCE_HEADER_PREFIX + "Content-Type",
            "application/json"
        );

        return setUpPollingClient(url, properties, setUpGetRequestFactory(properties));
    }

    private GetRequestFactory setUpGetRequestFactory(Properties properties) {
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

        return new GetRequestFactory(
            new GenericGetQueryCreator(lookupRow),
            HttpHeaderUtils.createDefaultHeaderPreprocessor(),
            HttpLookupConfig.builder()
                .url(getBaseUrl())
                .properties(properties)
                .build()
        );
    }

    private BodyBasedRequestFactory setUpBodyRequestFactory(
            String methodName,
            Properties properties) {

        SerializationSchema<RowData> jsonSerializer =
            new JsonFormatFactory()
                .createEncodingFormat(dynamicTableFactoryContext, new Configuration())
                .createRuntimeEncoder(null, lookupPhysicalDataType);

        return new BodyBasedRequestFactory(
            methodName,
            new GenericJsonQueryCreator(jsonSerializer),
            HttpHeaderUtils.createDefaultHeaderPreprocessor(),
            HttpLookupConfig.builder()
                .url(getBaseUrl())
                .properties(properties)
                .build()
        );
    }

    private JavaNetHttpPollingClient setUpPollingClient(
            String url,
            Properties properties,
            HttpRequestFactory requestFactory) {

        HttpLookupConfig lookupConfig = HttpLookupConfig.builder()
            .url(url)
            .properties(properties)
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

    private StubMapping setupServerStubForBasicAuth() {
        return wireMockServer.stubFor(get(urlEqualTo(ENDPOINT + "?id=1&uuid=2"))
                .withHeader("Content-Type", equalTo("application/json"))
                .withBasicAuth("user", "password")
                .willReturn(
                    aResponse()
                        .withStatus(200)
                        .withBody(readTestFile(SAMPLES_FOLDER + "HttpResult.json"))));
    }

    private static Properties prepareErrorCodeProperties(String errorCodeList, String whiteList) {
        Properties properties = new Properties();
        properties.setProperty(
            HttpConnectorConfigConstants.HTTP_ERROR_SOURCE_LOOKUP_CODE_WHITE_LIST,
            whiteList
        );
        properties.setProperty(
            HttpConnectorConfigConstants.HTTP_ERROR_SOURCE_LOOKUP_CODES_LIST,
            errorCodeList
        );

        properties.setProperty(
            HttpConnectorConfigConstants.LOOKUP_SOURCE_HEADER_PREFIX + "Content-Type",
            "application/json");

        return properties;
    }
}
