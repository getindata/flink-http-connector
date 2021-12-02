package com.getindata.connectors.http;

import static org.assertj.core.api.Assertions.assertThat;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

@Slf4j
class ExceptionUtilsTest {

  @Test
  void shouldConvertStackTrace() {
    String stringifyException =
        ExceptionUtils.stringifyException(new RuntimeException("Test Exception"));
    assertThat(stringifyException).contains("java.lang.RuntimeException: Test Exception");
  }
}
