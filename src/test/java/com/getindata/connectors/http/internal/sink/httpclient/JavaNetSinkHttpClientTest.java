package com.getindata.connectors.http.internal.sink.httpclient;

import java.net.http.HttpClient;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

import com.getindata.connectors.http.HttpPostRequestCallback;
import com.getindata.connectors.http.internal.HeaderPreprocessor;
import com.getindata.connectors.http.internal.SinkHttpClientResponse;
import com.getindata.connectors.http.internal.config.HttpConnectorConfigConstants;
import com.getindata.connectors.http.internal.config.ResponseItemStatus;
import com.getindata.connectors.http.internal.sink.HttpSinkRequestEntry;
import com.getindata.connectors.http.internal.table.sink.Slf4jHttpPostRequestCallback;
import com.getindata.connectors.http.internal.utils.HttpHeaderUtils;
import static com.getindata.connectors.http.TestHelper.assertPropertyArray;

@ExtendWith(MockitoExtension.class)
class JavaNetSinkHttpClientTest {

    private static MockedStatic<HttpClient> httpClientStaticMock;

    @Mock
    private HttpClient.Builder httpClientBuilder;
    @BeforeAll
    public static void beforeAll() {
        httpClientStaticMock = mockStatic(HttpClient.class);
    }

    protected HeaderPreprocessor headerPreprocessor;

    protected HttpPostRequestCallback<HttpRequest> postRequestCallback;

    @AfterAll
    public static void afterAll() {
        if (httpClientStaticMock != null) {
            httpClientStaticMock.close();
        }
    }

    @BeforeEach
    public void setUp() {
        postRequestCallback = new Slf4jHttpPostRequestCallback();
        headerPreprocessor = HttpHeaderUtils.createBasicAuthorizationHeaderPreprocessor();
        httpClientStaticMock.when(HttpClient::newBuilder).thenReturn(httpClientBuilder);
        lenient().when(httpClientBuilder.followRedirects(any())).thenReturn(httpClientBuilder);
        lenient().when(httpClientBuilder.sslContext(any())).thenReturn(httpClientBuilder);
        lenient().when(httpClientBuilder.executor(any())).thenReturn(httpClientBuilder);
    }

    private static Stream<Arguments> provideSubmitterFactory() {
        return Stream.of(
            Arguments.of(new PerRequestRequestSubmitterFactory()),
            Arguments.of(new BatchRequestSubmitterFactory(50))
        );
    }

    @ParameterizedTest
    @MethodSource("provideSubmitterFactory")
    public void shouldBuildClientWithoutHeaders(RequestSubmitterFactory requestSubmitterFactory) {

        JavaNetSinkHttpClient client =
            new JavaNetSinkHttpClient(
                new Properties(),
                postRequestCallback,
                this.headerPreprocessor,
                requestSubmitterFactory
            );
        assertThat(client.getHeadersAndValues()).isEmpty();
    }

    @ParameterizedTest
    @MethodSource("provideSubmitterFactory")
    public void shouldBuildClientWithHeaders(RequestSubmitterFactory requestSubmitterFactory) {

        // GIVEN
        Properties properties = new Properties();
        properties.setProperty("property", "val1");
        properties.setProperty("my.property", "val2");
        properties.setProperty(
            HttpConnectorConfigConstants.SINK_HEADER_PREFIX + "Origin",
            "https://developer.mozilla.org")
        ;
        properties.setProperty(
            HttpConnectorConfigConstants.SINK_HEADER_PREFIX + "Cache-Control",
            "no-cache, no-store, max-age=0, must-revalidate"
        );
        properties.setProperty(
            HttpConnectorConfigConstants.SINK_HEADER_PREFIX + "Access-Control-Allow-Origin",
            "*"
        );

        // WHEN
        JavaNetSinkHttpClient client = new JavaNetSinkHttpClient(
            properties,
            postRequestCallback,
            headerPreprocessor,
            requestSubmitterFactory
        );
        String[] headersAndValues = client.getHeadersAndValues();
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
    public void shouldHandleEmptyResponse() throws Exception {
        // GIVEN
        HttpSinkRequestEntry requestEntry = new HttpSinkRequestEntry("POST", "test data".getBytes());
        List<HttpSinkRequestEntry> requestEntries = Collections.singletonList(requestEntry);

        // Create an HttpRequest (which wraps java.net.http.HttpRequest)
        java.net.http.HttpRequest javaHttpRequest = java.net.http.HttpRequest.newBuilder()
            .uri(java.net.URI.create("http://test.com/endpoint"))
            .POST(java.net.http.HttpRequest.BodyPublishers.ofByteArray(requestEntry.element))
            .build();
        HttpRequest httpRequest = new HttpRequest(
            javaHttpRequest,
            List.of(requestEntry.element),
            requestEntry.method
        );

        // Create a response wrapper with an empty response (null)
        JavaNetHttpResponseWrapper responseWrapper = new JavaNetHttpResponseWrapper(httpRequest, null);

        // Mock RequestSubmitterFactory to return a mock RequestSubmitter
        RequestSubmitterFactory mockFactory = mock(RequestSubmitterFactory.class);
        RequestSubmitter mockSubmitter = mock(RequestSubmitter.class);
        when(mockFactory.createSubmitter(any(Properties.class), any(String[].class)))
            .thenReturn(mockSubmitter);

        // Mock the submit method to return a completed future with the empty response
        CompletableFuture<JavaNetHttpResponseWrapper> responseFuture =
            CompletableFuture.completedFuture(responseWrapper);
        when(mockSubmitter.submit(anyString(), eq(requestEntries)))
            .thenReturn(Collections.singletonList(responseFuture));

        // WHEN
        JavaNetSinkHttpClient client = new JavaNetSinkHttpClient(
            new Properties(),
            postRequestCallback,
            headerPreprocessor,
            mockFactory
        );

        SinkHttpClientResponse response = client.putRequests(requestEntries, "http://test.com/endpoint").get();

        // THEN
        assertThat(response.getRequests()).hasSize(1);
        assertThat(response.getRequests().get(0).getStatus()).isEqualTo(ResponseItemStatus.TEMPORAL);
        assertThat(response.getTemporalRequests()).hasSize(1);
        assertThat(response.getSuccessfulRequests()).isEmpty();
        assertThat(response.getFailedRequests()).isEmpty();
        assertThat(response.getIgnoredRequests()).isEmpty();
    }

}
