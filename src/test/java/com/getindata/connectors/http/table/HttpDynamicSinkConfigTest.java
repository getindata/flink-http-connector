package com.getindata.connectors.http.table;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class HttpDynamicSinkConfigTest {
  @Test
  public void contentTypeTest() {
    var jsonSinkConfig = HttpDynamicSinkConfig
        .builder()
        .url("localhost:8000")
        .format("json")
        .insertMethod("POST")
        .build();
    var unexpectedSinkConfig = HttpDynamicSinkConfig
        .builder()
        .url("localhost:8000")
        .format("abcd")
        .insertMethod("POST")
        .build();

    assertEquals("application/json", jsonSinkConfig.getContentType());
    assertEquals("application/abcd", unexpectedSinkConfig.getContentType());
  }
}
