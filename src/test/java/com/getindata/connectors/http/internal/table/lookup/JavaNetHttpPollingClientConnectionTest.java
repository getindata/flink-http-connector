package com.getindata.connectors.http.internal.table.lookup;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.Stream;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.matching.RequestPatternBuilder;
import com.github.tomakehurst.wiremock.stubbing.StubMapping;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.formats.json.JsonFormatFactory;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
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
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static org.assertj.core.api.Assertions.assertThat;

import com.getindata.connectors.http.LookupArg;
import com.getindata.connectors.http.internal.config.HttpConnectorConfigConstants;
import com.getindata.connectors.http.internal.table.lookup.querycreators.GenericGetQueryCreator;
import static com.getindata.connectors.http.TestHelper.readTestFile;

@ExtendWith(MockitoExtension.class)
class JavaNetHttpPollingClientConnectionTest {

    private static final String SAMPLES_FOLDER = "/http/";

    private static final List<String> LOOKUP_KEYS = List.of("id", "uuid");

    private static WireMockServer wireMockServer;

    @Mock
    private Context dynamicTableFactoryContext;

    private DynamicTableSource.Context dynamicTableSourceContext;

    private StubMapping stubMapping;

    private JavaNetHttpPollingClientFactory pollingClientFactory;

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
        this.pollingClientFactory = new JavaNetHttpPollingClientFactory();
    }

    @AfterEach
    public void cleanUp() {
        if (stubMapping != null && wireMockServer != null) {
            wireMockServer.removeStub(stubMapping);
        }
    }

    @Test
    void shouldQuery200WithParams() {
        stubMapping = setupServerStub(200);

        JavaNetHttpPollingClient pollingClient = setUpPollingClient(getBaseUrl(), LOOKUP_KEYS);

        RowData result = pollingClient.pull(
            List.of(
                new LookupArg("id", "1"),
                new LookupArg("uuid", "2")
            )
        ).orElseThrow();

        wireMockServer.verify(RequestPatternBuilder.forCustomMatcher(stubMapping.getRequest()));

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
        stubMapping = setupServerStub(201);

        JavaNetHttpPollingClient pollingClient = setUpPollingClient(
            getBaseUrl(),
            LOOKUP_KEYS,
            properties
        );

        Optional<RowData> poll = pollingClient.pull(
            List.of(
                new LookupArg("id", "1"),
                new LookupArg("uuid", "2")
            )
        );

        assertThat(poll.isEmpty()).isEqualTo(isExpectedResponseEmpty);
    }

    @Test
    void shouldHandleServerError() {
        stubMapping = setupServerStub(500);

        JavaNetHttpPollingClient pollingClient = setUpPollingClient(getBaseUrl(), LOOKUP_KEYS);

        Optional<RowData> poll = pollingClient.pull(
            List.of(
                new LookupArg("id", "1"),
                new LookupArg("uuid", "2")
            )
        );

        wireMockServer.verify(RequestPatternBuilder.forCustomMatcher(stubMapping.getRequest()));

        assertThat(poll.isEmpty()).isTrue();
    }

    @Test
    void shouldProcessWithMissingArguments() {
        stubMapping = setupServerStub(200);

        JavaNetHttpPollingClient pollingClient =
            setUpPollingClient(getBaseUrl(), LOOKUP_KEYS);

        Optional<RowData> poll = pollingClient.pull(Collections.emptyList());

        assertThat(poll.isEmpty()).isTrue();
    }

    @ParameterizedTest
    @ValueSource(strings = {
        "user:password",
        "Basic dXNlcjpwYXNzd29yZA=="
    })
    public void shouldConnectWithBasicAuth(String authorizationHeaderValue) {

        stubMapping = setupServerStubForBasicAuth();

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
            LOOKUP_KEYS,
            properties
        );

        RowData result = pollingClient.pull(
            List.of(
                new LookupArg("id", "1"),
                new LookupArg("uuid", "2")
            )
        ).orElseThrow();

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
        return wireMockServer.baseUrl() + "/service";
    }

    public JavaNetHttpPollingClient setUpPollingClient(String url, List<String> arguments) {

        Properties properties = new Properties();
        properties.setProperty(
            HttpConnectorConfigConstants.LOOKUP_SOURCE_HEADER_PREFIX + "Content-Type",
            "application/json"
        );

        return setUpPollingClient(url, arguments, properties);
    }

    private JavaNetHttpPollingClient setUpPollingClient(
            String url,
            List<String> arguments,
            Properties properties) {
        HttpLookupConfig lookupConfig = HttpLookupConfig.builder()
            .url(url)
            .arguments(arguments)
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

        return pollingClientFactory
            .createPollClient(lookupConfig, schemaDecoder, new GenericGetQueryCreator());
    }

    private StubMapping setupServerStub(int status) {
        return wireMockServer.stubFor(
            get(urlEqualTo("/service?id=1&uuid=2"))
                .withHeader("Content-Type", equalTo("application/json"))
                .willReturn(
                    aResponse()
                        .withStatus(status)
                        .withBody(readTestFile(SAMPLES_FOLDER + "HttpResult.json"))));
    }

    private StubMapping setupServerStubForBasicAuth() {
        return wireMockServer.stubFor(get(urlEqualTo("/service?id=1&uuid=2"))
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
