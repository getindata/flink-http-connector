package com.getindata.connectors.http.internal.sink;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;

import org.apache.flink.connector.base.sink.writer.ElementConverter;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.getindata.connectors.http.HttpSink;
import com.getindata.connectors.http.internal.SinkHttpClient;
import com.getindata.connectors.http.internal.SinkHttpClientResponse;

public class HttpSinkBuilderTest {

    private static final ElementConverter<String, HttpSinkRequestEntry> ELEMENT_CONVERTER =
        (s, context) -> new HttpSinkRequestEntry("POST", s.getBytes(StandardCharsets.UTF_8));

    @Test
    public void testEmptyUrl() {
        assertThrows(
            IllegalArgumentException.class,
            () -> HttpSink.<String>builder()
                .setElementConverter(ELEMENT_CONVERTER)
                .setSinkHttpClientBuilder(MockHttpClient::new)
                .setEndpointUrl("")
                .build()
        );
    }

    @Test
    public void testNullUrl() {
        assertThrows(
            IllegalArgumentException.class,
            () -> HttpSink.<String>builder()
                .setElementConverter(ELEMENT_CONVERTER)
                .setSinkHttpClientBuilder(MockHttpClient::new)
                .build()
        );
    }

    @Test
    public void testNullHttpClient() {
        assertThrows(
            NullPointerException.class,
            () -> HttpSink.<String>builder()
                .setElementConverter(ELEMENT_CONVERTER)
                .setSinkHttpClientBuilder(null)
                .setEndpointUrl("localhost:8000")
                .build()
        );
    }

    private static class MockHttpClient implements SinkHttpClient {

        MockHttpClient(Properties properties) {

        }

        @Override
        public CompletableFuture<SinkHttpClientResponse> putRequests(
            List<HttpSinkRequestEntry> requestEntries, String endpointUrl
        ) {
            throw new RuntimeException("Mock implementation of HttpClient");
        }
    }
}
