package com.getindata.connectors.http.internal.sink.httpclient;

import java.net.http.HttpClient;
import java.util.Properties;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

import com.getindata.connectors.http.internal.config.HttpConnectorConfigConstants;
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

    @AfterAll
    public static void afterAll() {
        if (httpClientStaticMock != null) {
            httpClientStaticMock.close();
        }
    }

    @BeforeEach
    public void setUp() {
        httpClientStaticMock.when(HttpClient::newBuilder).thenReturn(httpClientBuilder);
        when(httpClientBuilder.followRedirects(any())).thenReturn(httpClientBuilder);
        when(httpClientBuilder.sslContext(any())).thenReturn(httpClientBuilder);
    }

    @Test
    public void shouldBuildClientWithoutHeaders() {

        JavaNetSinkHttpClient client = new JavaNetSinkHttpClient(new Properties());
        assertThat(client.getHeadersAndValues()).isEmpty();
    }

    @Test
    public void shouldBuildClientWithHeaders() {

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
        JavaNetSinkHttpClient client = new JavaNetSinkHttpClient(properties);
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

}
