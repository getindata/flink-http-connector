package com.getindata.connectors.http.internal;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import static org.assertj.core.api.Assertions.assertThat;

public class ComposeHeaderPreprocessorTest {
    @ParameterizedTest
    @CsvSource({
        "a, a",
        "a123, a123",
        "user:password, user:password",
        "Basic dXNlcjpwYXNzd29yZA==, Basic dXNlcjpwYXNzd29yZA=="
    })
    public void testNoPreprocessors(String rawValue, String expectedValue) {
        var noPreprocessorHeaderPreprocessor = new ComposeHeaderPreprocessor(null);
        var obtainedValue = noPreprocessorHeaderPreprocessor
            .preprocessValueForHeader("someHeader", rawValue);
        assertThat(obtainedValue).isEqualTo(expectedValue);
    }
}
