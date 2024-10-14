package com.getindata.connectors.http.internal.status;

import java.util.List;
import java.util.Properties;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.getindata.connectors.http.internal.status.ComposeHttpStatusCodeChecker.ComposeHttpStatusCodeCheckerConfig;
import static com.getindata.connectors.http.internal.status.HttpResponseStatus.FAILURE_NOT_RETRYABLE;
import static com.getindata.connectors.http.internal.status.HttpResponseStatus.FAILURE_RETRYABLE;
import static com.getindata.connectors.http.internal.status.HttpResponseStatus.SUCCESS;

class ComposeHttpStatusCodeCheckerTest {

    private static final String NOT_RETRYABLE_CODE_PROPERTY = "error.code";
    private static final String NOT_RETRYABLE_WHITELIST_PROPERTY = "error.code.exclude";
    private static final String RETRYABLE_CODE_PROPERTY = "retryable.code";
    private static final String RETRYABLE_WHITELIST_PROPERTY = "retryable.code.exclude";

    @Test
    void shouldReturnAppropriateStatusByDefault() {
        Properties properties = new Properties();
        ComposeHttpStatusCodeCheckerConfig checkerConfig = prepareCheckerConfig(properties);
        HttpStatusCodeChecker codeChecker = new ComposeHttpStatusCodeChecker(checkerConfig);

        assertAll(() -> {
            assertThat(codeChecker.checkStatus(100)).isEqualTo(SUCCESS);
            assertThat(codeChecker.checkStatus(200)).isEqualTo(SUCCESS);
            assertThat(codeChecker.checkStatus(302)).isEqualTo(SUCCESS);
            assertThat(codeChecker.checkStatus(400)).isEqualTo(FAILURE_NOT_RETRYABLE);
            assertThat(codeChecker.checkStatus(404)).isEqualTo(FAILURE_NOT_RETRYABLE);
            assertThat(codeChecker.checkStatus(500)).isEqualTo(FAILURE_RETRYABLE);
            assertThat(codeChecker.checkStatus(501)).isEqualTo(FAILURE_RETRYABLE);
            assertThat(codeChecker.checkStatus(503)).isEqualTo(FAILURE_RETRYABLE);
            assertThat(codeChecker.checkStatus(505)).isEqualTo(FAILURE_RETRYABLE);
        });
    }

    @Test
    void shouldReturnAppropriateStatus() {
        Properties properties = new Properties();
        properties.setProperty(NOT_RETRYABLE_CODE_PROPERTY, "1XX,4XX,505");
        properties.setProperty(NOT_RETRYABLE_WHITELIST_PROPERTY, "404");
        properties.setProperty(RETRYABLE_CODE_PROPERTY, "404,5XX");
        properties.setProperty(RETRYABLE_WHITELIST_PROPERTY, "501,505");

        ComposeHttpStatusCodeCheckerConfig checkerConfig = prepareCheckerConfig(properties);
        HttpStatusCodeChecker codeChecker = new ComposeHttpStatusCodeChecker(checkerConfig);

        assertAll(() -> {
            assertThat(codeChecker.checkStatus(100)).isEqualTo(FAILURE_NOT_RETRYABLE);
            assertThat(codeChecker.checkStatus(200)).isEqualTo(SUCCESS);
            assertThat(codeChecker.checkStatus(400)).isEqualTo(FAILURE_NOT_RETRYABLE);
            assertThat(codeChecker.checkStatus(404)).isEqualTo(FAILURE_RETRYABLE);
            assertThat(codeChecker.checkStatus(500)).isEqualTo(FAILURE_RETRYABLE);
            assertThat(codeChecker.checkStatus(501)).isEqualTo(SUCCESS);
            assertThat(codeChecker.checkStatus(503)).isEqualTo(FAILURE_RETRYABLE);
            assertThat(codeChecker.checkStatus(505)).isEqualTo(FAILURE_NOT_RETRYABLE);
        });
    }

    @Test
    void shouldParseWhiteList() {
        String rawCodes = "403, 100, 200, 300, 303, 200";
        List<Integer> whitelistCodes = List.of(403, 100, 200, 300, 303, 200);
        Properties properties = new Properties();
        properties.setProperty(NOT_RETRYABLE_CODE_PROPERTY, "1XX, 2XX, 3XX, 4XX");
        properties.setProperty(NOT_RETRYABLE_WHITELIST_PROPERTY, rawCodes);
        properties.setProperty(RETRYABLE_CODE_PROPERTY, "5XX");
        properties.setProperty(RETRYABLE_WHITELIST_PROPERTY, rawCodes);

        ComposeHttpStatusCodeCheckerConfig checkerConfig = prepareCheckerConfig(properties);
        HttpStatusCodeChecker codeChecker = new ComposeHttpStatusCodeChecker(checkerConfig);

        assertAll(() -> {
            whitelistCodes.forEach(
                code -> assertThat(codeChecker.checkStatus(code)).isEqualTo(SUCCESS)
            );

            assertThat(codeChecker.checkStatus(301))
                .withFailMessage(
                    "Not on a white list but matches 3XX range. "
                        + "Should be considered as error code.")
                .isEqualTo(FAILURE_NOT_RETRYABLE);
        });
    }

    @Test
    void shouldParseErrorCodeList() {
        List<Integer> notRetryableCodes = List.of(100, 202, 404);
        List<Integer> retryableCodes = List.of(302, 502);
        Properties properties = new Properties();
        properties.setProperty(NOT_RETRYABLE_CODE_PROPERTY, "100, 202, 404");
        properties.setProperty(RETRYABLE_CODE_PROPERTY, "302, 502");

        ComposeHttpStatusCodeCheckerConfig checkerConfig = prepareCheckerConfig(properties);
        HttpStatusCodeChecker codeChecker = new ComposeHttpStatusCodeChecker(checkerConfig);

        assertAll(() -> {
            notRetryableCodes.forEach(code -> assertThat(codeChecker.checkStatus(code))
                .isEqualTo(FAILURE_NOT_RETRYABLE));
            retryableCodes.forEach(code -> assertThat(codeChecker.checkStatus(code))
                .isEqualTo(FAILURE_RETRYABLE));
        });
    }

    @Test
    void shouldParseErrorCodeRange() {
        Properties properties = new Properties();
        properties.setProperty(NOT_RETRYABLE_CODE_PROPERTY, "1XX, 2XX");
        properties.setProperty(RETRYABLE_CODE_PROPERTY, "3XX, 4XX");
        List<Integer> notRetryableCodes = List.of(100, 110, 200, 220);
        List<Integer> retryableCodes = List.of(301, 404);

        ComposeHttpStatusCodeCheckerConfig checkerConfig = prepareCheckerConfig(properties);
        HttpStatusCodeChecker codeChecker = new ComposeHttpStatusCodeChecker(checkerConfig);

        assertAll(() -> {
            notRetryableCodes.forEach(code -> assertThat(codeChecker.checkStatus(code))
                .isEqualTo(FAILURE_NOT_RETRYABLE));
            retryableCodes.forEach(code -> assertThat(codeChecker.checkStatus(code))
                .isEqualTo(FAILURE_RETRYABLE));
            assertThat(codeChecker.checkStatus(503))
                .withFailMessage(
                    "Out ot Error code type range therefore should be not marked as error code.")
                .isEqualTo(SUCCESS);
        });
    }

    @Test
    void shouldIgnoreRedundantWhiteSpacesOrEmptyOrRepeatedValues() {
        Properties properties = new Properties();
        properties.setProperty(NOT_RETRYABLE_CODE_PROPERTY, " , 100,200, 300, , 303 ,200 ");
        properties.setProperty(RETRYABLE_CODE_PROPERTY, ",5XX, 4XX,,  ,");
        List<Integer> notRetryableCodes = List.of(100, 200, 300, 303);
        List<Integer> retryableCodes = List.of(500, 501, 400, 401);

        ComposeHttpStatusCodeCheckerConfig checkerConfig = prepareCheckerConfig(properties);
        HttpStatusCodeChecker codeChecker = new ComposeHttpStatusCodeChecker(checkerConfig);

        assertAll(() -> {
            notRetryableCodes.forEach(code -> assertThat(codeChecker.checkStatus(code))
                .isEqualTo(FAILURE_NOT_RETRYABLE));
            retryableCodes.forEach(code -> assertThat(codeChecker.checkStatus(code))
                .isEqualTo(FAILURE_RETRYABLE));
        });
    }


    @ParameterizedTest
    @ValueSource(strings = {"X", "XXX", " X X", "1X1", "XX1", "XX1XX", "XX1 XX"})
    void shouldThrowOnInvalidCodeRangeInNonRetryableError(String listCode) {
        Properties properties = new Properties();
        properties.setProperty(NOT_RETRYABLE_CODE_PROPERTY, listCode);

        ComposeHttpStatusCodeCheckerConfig checkerConfig = prepareCheckerConfig(properties);

        assertThrows(
            Exception.class,
            () -> new ComposeHttpStatusCodeChecker(checkerConfig)
        );
    }

    @ParameterizedTest
    @ValueSource(strings = {"X", "XXX", " X X", "1X1", "XX1", "XX1XX", "XX1 XX"})
    void shouldThrowOnInvalidCodeRangeInRetryableError(String listCode) {
        Properties properties = new Properties();
        properties.setProperty(RETRYABLE_CODE_PROPERTY, listCode);

        ComposeHttpStatusCodeCheckerConfig checkerConfig = prepareCheckerConfig(properties);

        assertThrows(
            Exception.class,
            () -> new ComposeHttpStatusCodeChecker(checkerConfig)
        );
    }

    private ComposeHttpStatusCodeCheckerConfig prepareCheckerConfig(Properties properties) {
        return ComposeHttpStatusCodeCheckerConfig.builder()
            .properties(properties)
            .errorCodePrefix(NOT_RETRYABLE_CODE_PROPERTY)
            .errorWhiteListPrefix(NOT_RETRYABLE_WHITELIST_PROPERTY)
            .retryableCodePrefix(RETRYABLE_CODE_PROPERTY)
            .retryableWhiteListPrefix(RETRYABLE_WHITELIST_PROPERTY)
            .build();
    }
}
