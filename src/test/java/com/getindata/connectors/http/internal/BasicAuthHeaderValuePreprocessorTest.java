package com.getindata.connectors.http.internal;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import static org.assertj.core.api.Assertions.assertThat;

class BasicAuthHeaderValuePreprocessorTest {

    @ParameterizedTest
    @CsvSource({
        "user:password, Basic dXNlcjpwYXNzd29yZA==, false",
        "Basic dXNlcjpwYXNzd29yZA==, Basic dXNlcjpwYXNzd29yZA==, false",
        "abc123, abc123, true",
        "Basic dXNlcjpwYXNzd29yZA==, Basic dXNlcjpwYXNzd29yZA==, true",
        "Bearer dXNlcjpwYXNzd29yZA==, Bearer dXNlcjpwYXNzd29yZA==, true"
    })
    public void testAuthorizationHeaderPreprocess(
            String headerRawValue,
            String expectedHeaderValue,
            boolean useRawAuthHeader) {
        BasicAuthHeaderValuePreprocessor preprocessor =
            new BasicAuthHeaderValuePreprocessor(useRawAuthHeader);
        String headerValue = preprocessor.preprocessHeaderValue(headerRawValue);
        assertThat(headerValue).isEqualTo(expectedHeaderValue);
    }
}
