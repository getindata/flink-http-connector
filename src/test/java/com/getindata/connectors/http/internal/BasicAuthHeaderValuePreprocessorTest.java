package com.getindata.connectors.http.internal;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import static org.assertj.core.api.Assertions.assertThat;

class BasicAuthHeaderValuePreprocessorTest {

    private BasicAuthHeaderValuePreprocessor preprocessor;

    @BeforeEach
    public void setUp() {
        this.preprocessor = new BasicAuthHeaderValuePreprocessor();
    }

    @ParameterizedTest
    @CsvSource({
        "user:password, Basic dXNlcjpwYXNzd29yZA==",
        "Basic dXNlcjpwYXNzd29yZA==, Basic dXNlcjpwYXNzd29yZA=="
    })
    public void testAuthorizationHeaderPreprocess(
            String headerRawValue,
            String expectedHeaderValue) {
        String headerValue = this.preprocessor.preprocessHeaderValue(headerRawValue);
        assertThat(headerValue).isEqualTo(expectedHeaderValue);
    }
}
