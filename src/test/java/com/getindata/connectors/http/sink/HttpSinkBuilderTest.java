package com.getindata.connectors.http.sink;

import org.junit.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.Assert.assertThrows;

public class HttpSinkBuilderTest {
  @Test
  public void testEmptyUrl() {
    assertThrows(
        IllegalArgumentException.class,
        () -> HttpSink.<String>builder()
                .setElementConverter((s, context) -> new HttpSinkRequestEntry(
                    "POST",
                    "text/html",
                    s.getBytes(
                        StandardCharsets.UTF_8)
                ))
            .setEndpointUrl("")
            .build()
    );
  }

  @Test
  public void testNullUrl() {
    assertThrows(
        NullPointerException.class,
        () -> HttpSink.<String>builder()
                      .setElementConverter((s, context) -> new HttpSinkRequestEntry(
                          "POST",
                          "text/html",
                          s.getBytes(
                              StandardCharsets.UTF_8)
                      ))
                      .build()
    );
  }
}
