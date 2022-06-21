package com.getindata.connectors.http.sink;

import org.apache.flink.connector.base.sink.writer.ElementConverter;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertThrows;

public class HttpSinkBuilderTest {
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
                      .build()
    );
  }
}
