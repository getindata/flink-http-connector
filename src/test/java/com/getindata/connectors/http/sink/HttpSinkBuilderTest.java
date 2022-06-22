package com.getindata.connectors.http.sink;

import com.getindata.connectors.http.SinkHttpClient;
import com.getindata.connectors.http.SinkHttpClientResponse;
import org.apache.flink.connector.base.sink.writer.ElementConverter;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertThrows;

public class HttpSinkBuilderTest {
  private static class MockHttpClient implements SinkHttpClient {
    @Override
    public CompletableFuture<SinkHttpClientResponse> putRequests(
        List<HttpSinkRequestEntry> requestEntries, String endpointUrl
    ) {
      throw new RuntimeException("Mock implementation of HttpClient");
    }
  }

  private static final ElementConverter<String, HttpSinkRequestEntry> ELEMENT_CONVERTER =
      (s, context) -> new HttpSinkRequestEntry(
          "POST",
          "text/html",
          s.getBytes(
              StandardCharsets.UTF_8)
      );

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
                      .setEndpointUrl("localhost:8000")
                      .build()
    );
  }
}
