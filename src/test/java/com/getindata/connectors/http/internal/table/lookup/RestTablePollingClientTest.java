package com.getindata.connectors.http.internal.table.lookup;

import java.net.http.HttpClient;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.matching.RequestPatternBuilder;
import com.github.tomakehurst.wiremock.stubbing.StubMapping;
import org.apache.flink.table.data.RowData;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static org.assertj.core.api.Assertions.assertThat;

import com.getindata.connectors.http.internal.JsonResultTableConverter;
import com.getindata.connectors.http.internal.JsonResultTableConverter.HttpResultConverterOptions;
import static com.getindata.connectors.http.TestHelper.readTestFile;

class RestTablePollingClientTest {

    private static final String SAMPLES_FOLDER = "/http/";

    private static final List<String> columnNames =
        List.of("id", "msg", "uuid", "isActive", "balance");

    private static WireMockServer wireMockServer;

    private static HttpResultConverterOptions converterOptions;

    private StubMapping stubMapping;

    @BeforeAll
    public static void setUpAll() {
        wireMockServer = new WireMockServer();
        wireMockServer.start();

        converterOptions =
            HttpResultConverterOptions.builder()
                .columnNames(columnNames)
                .aliases(
                    Map.of(
                        "isActive", "$.details.isActive",
                        "balance", "$.details.nestedDetails.balance"))
                .build();
    }

    @AfterAll
    public static void cleanUpAll() {
        wireMockServer.stop();
    }

    @AfterEach
    public void cleanUp() {
        if (stubMapping != null) {
            wireMockServer.removeStub(stubMapping);
        }
    }

    @Test
    void shouldQuery200WithParams() {
        stubMapping = setupServerStub("/service?id=1&uuid=2", 200);

        RestTablePollingClient pollingClient =
            setUpPollingClient(getBaseUrl(), List.of("id", "uuid"));
        RowData poll =
            pollingClient.pull(List.of(new LookupArg("id", "1"), new LookupArg("uuid", "2")));

        wireMockServer.verify(RequestPatternBuilder.forCustomMatcher(stubMapping.getRequest()));

        assertThat(poll.getArity()).isEqualTo(columnNames.size());
        assertThat(poll.getString(3).toString()).isEqualTo("true");
        assertThat(poll.getString(4).toString()).isEqualTo("$1,729.34");
    }

    @Test
    void shouldHandleNot200() {
        stubMapping = setupServerStub("/service?id=1&uuid=2", 201);

        RestTablePollingClient pollingClient =
            setUpPollingClient(getBaseUrl(), List.of("id", "uuid"));
        RowData poll =
            pollingClient.pull(List.of(new LookupArg("id", "1"), new LookupArg("uuid", "2")));

        wireMockServer.verify(RequestPatternBuilder.forCustomMatcher(stubMapping.getRequest()));

        assertThat(poll.getArity()).isEqualTo(columnNames.size());
        assertThat(poll.getString(3)).isNull();
        assertThat(poll.getString(4)).isNull();
    }

    @Test
    void shouldHandleServerError() {
        stubMapping = setupServerStub("/service?id=1&uuid=2", 500);

        RestTablePollingClient pollingClient =
            setUpPollingClient(getBaseUrl(), List.of("id", "uuid"));
        RowData poll =
            pollingClient.pull(List.of(new LookupArg("id", "1"), new LookupArg("uuid", "2")));

        wireMockServer.verify(RequestPatternBuilder.forCustomMatcher(stubMapping.getRequest()));

        assertThat(poll.getArity()).isEqualTo(columnNames.size());
        assertThat(poll.getString(3)).isNull();
        assertThat(poll.getString(4)).isNull();
    }

    @Test
    void shouldProcessWithMissingArguments() {
        stubMapping = setupServerStub("/service?id=1&uuid=2", 200);

        RestTablePollingClient pollingClient =
            setUpPollingClient(getBaseUrl(), List.of("id", "uuid"));
        RowData poll = pollingClient.pull(Collections.emptyList());

        assertThat(poll.getArity()).isEqualTo(columnNames.size());
        assertThat(poll.getString(3)).isNull();
        assertThat(poll.getString(4)).isNull();
    }

    private String getBaseUrl() {
        return wireMockServer.baseUrl() + "/service";
    }

    public RestTablePollingClient setUpPollingClient(String url, List<String> arguments) {

        HttpLookupConfig expectedLookupConfig =
            HttpLookupConfig.builder().columnNames(columnNames).url(url).arguments(arguments)
                .build();

        return new RestTablePollingClient(
            new JsonResultTableConverter(converterOptions),
            expectedLookupConfig,
            HttpClient.newHttpClient());
    }

    private StubMapping setupServerStub(String paramsPath, int status) {
        return wireMockServer.stubFor(
            get(urlEqualTo(paramsPath))
                .willReturn(
                    aResponse()
                        .withStatus(status)
                        .withBody(readTestFile(SAMPLES_FOLDER + "HttpResult.json"))));
    }
}
