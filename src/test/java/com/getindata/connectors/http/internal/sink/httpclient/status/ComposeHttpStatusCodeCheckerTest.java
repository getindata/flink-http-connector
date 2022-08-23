package com.getindata.connectors.http.internal.sink.httpclient.status;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.flink.util.StringUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.getindata.connectors.http.internal.config.HttpConnectorConfigConstants;
import com.getindata.connectors.http.internal.status.ComposeHttpStatusCodeChecker;
import com.getindata.connectors.http.internal.status.ComposeHttpStatusCodeChecker.ComposeHttpStatusCodeCheckerConfig;

class ComposeHttpStatusCodeCheckerTest {

    private static final String STRING_CODES = "403, 100,200, 300, , 303 ,200";

    private static final List<Integer> CODES =
        Arrays.stream(STRING_CODES.split(HttpConnectorConfigConstants.ERROR_CODE_DELIM))
            .filter(code -> !StringUtils.isNullOrWhitespaceOnly(code))
            .map(String::trim)
            .mapToInt(Integer::parseInt)
            .boxed()
            .collect(Collectors.toList());

    private ComposeHttpStatusCodeChecker codeChecker;

    @BeforeAll
    public static void beforeAll() {
        assertThat(CODES).isNotEmpty();
    }

    private static Stream<Arguments> propertiesArguments() {
        return Stream.of(
            Arguments.of(new Properties()),
            Arguments.of(prepareErrorCodeProperties("", "")),
            Arguments.of(prepareErrorCodeProperties(" ", " ")),
            Arguments.of(prepareErrorCodeProperties(",,,", ",,,,"))
        );
    }

    @ParameterizedTest
    @MethodSource("propertiesArguments")
    public void shouldPassOnDefault(Properties properties) {

        ComposeHttpStatusCodeCheckerConfig checkerConfig = prepareCheckerConfig(properties);

        codeChecker = new ComposeHttpStatusCodeChecker(checkerConfig);

        assertAll(() -> {
            assertThat(codeChecker.isErrorCode(100)).isFalse();
            assertThat(codeChecker.isErrorCode(200)).isFalse();
            assertThat(codeChecker.isErrorCode(500)).isTrue();
            assertThat(codeChecker.isErrorCode(501)).isTrue();
            assertThat(codeChecker.isErrorCode(400)).isTrue();
            assertThat(codeChecker.isErrorCode(404)).isTrue();
        });
    }

    @Test
    public void shouldParseWhiteList() {

        Properties properties = new Properties();
        properties.setProperty(
            HttpConnectorConfigConstants.HTTP_ERROR_SINK_CODE_WHITE_LIST,
            STRING_CODES);
        properties.setProperty(
            HttpConnectorConfigConstants.HTTP_ERROR_SINK_CODES_LIST,
            "1XX, 2XX, 3XX, 4XX, 5XX"
        );

        ComposeHttpStatusCodeCheckerConfig checkerConfig = prepareCheckerConfig(properties);

        codeChecker = new ComposeHttpStatusCodeChecker(checkerConfig);

        assertAll(() -> {
            CODES.forEach(code -> assertThat(codeChecker.isErrorCode(code)).isFalse());

            assertThat(codeChecker.isErrorCode(301))
                .withFailMessage(
                    "Not on a white list but matches 3XX range. "
                        + "Should be considered as error code.")
                .isTrue();
        });
    }

    @Test
    public void shouldParseErrorCodeList() {

        Properties properties = new Properties();
        properties.setProperty(
            HttpConnectorConfigConstants.HTTP_ERROR_SINK_CODES_LIST,
            STRING_CODES);

        ComposeHttpStatusCodeCheckerConfig checkerConfig = prepareCheckerConfig(properties);

        codeChecker = new ComposeHttpStatusCodeChecker(checkerConfig);

        assertAll(() -> CODES.forEach(code -> assertThat(codeChecker.isErrorCode(code)).isTrue()));
    }

    @Test
    public void shouldParseErrorCodeRange() {

        Properties properties = new Properties();
        properties.setProperty(
            HttpConnectorConfigConstants.HTTP_ERROR_SINK_CODES_LIST, "1xx, 2XX ");

        List<Integer> codes = List.of(100, 110, 200, 220);

        ComposeHttpStatusCodeCheckerConfig checkerConfig = prepareCheckerConfig(properties);

        codeChecker = new ComposeHttpStatusCodeChecker(checkerConfig);

        assertAll(() -> {
            codes.forEach(code -> assertThat(codeChecker.isErrorCode(code)).isTrue());

            assertThat(codeChecker.isErrorCode(303))
                .withFailMessage(
                    "Out ot Error code type range therefore should be not marked as error code.")
                .isFalse();
        });
    }

    @ParameterizedTest
    @ValueSource(strings = {"X", "XXX", " X X", "1X1", "XX1", "XX1XX", "XX1 XX"})
    public void shouldThrowOnInvalidCodeRange(String listCode) {

        Properties properties = new Properties();
        properties.setProperty(
            HttpConnectorConfigConstants.HTTP_ERROR_SINK_CODES_LIST, listCode);

        ComposeHttpStatusCodeCheckerConfig checkerConfig = prepareCheckerConfig(properties);

        assertThrows(
            Exception.class,
            () -> new ComposeHttpStatusCodeChecker(checkerConfig)
        );
    }

    private static Properties prepareErrorCodeProperties(String errorCodeList, String whiteList) {
        Properties properties = new Properties();
        properties.setProperty(
            HttpConnectorConfigConstants.HTTP_ERROR_SINK_CODE_WHITE_LIST,
            whiteList
        );
        properties.setProperty(
            HttpConnectorConfigConstants.HTTP_ERROR_SINK_CODES_LIST,
            errorCodeList
        );
        return properties;
    }

    private ComposeHttpStatusCodeCheckerConfig prepareCheckerConfig(Properties properties) {
        return ComposeHttpStatusCodeCheckerConfig.builder()
            .properties(properties)
            .whiteListPrefix(HttpConnectorConfigConstants.HTTP_ERROR_SINK_CODE_WHITE_LIST)
            .errorCodePrefix(HttpConnectorConfigConstants.HTTP_ERROR_SINK_CODES_LIST)
            .build();
    }
}
