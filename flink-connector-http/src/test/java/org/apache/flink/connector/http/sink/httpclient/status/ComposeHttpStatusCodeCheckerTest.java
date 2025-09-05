/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.http.sink.httpclient.status;

import org.apache.flink.connector.http.config.HttpConnectorConfigConstants;
import org.apache.flink.connector.http.status.ComposeHttpStatusCodeChecker;
import org.apache.flink.connector.http.status.ComposeHttpStatusCodeChecker.ComposeHttpStatusCodeCheckerConfig;
import org.apache.flink.util.StringUtils;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Test for {@link ComposeHttpStatusCodeChecker}. */
class ComposeHttpStatusCodeCheckerTest {

    private static final String STRING_CODES = "403, 100,200, 300, , 303 ,200";

    private static final List<Integer> CODES =
            Arrays.stream(STRING_CODES.split(HttpConnectorConfigConstants.PROP_DELIM))
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
                Arguments.of(prepareErrorCodeProperties(",,,", ",,,,")));
    }

    @ParameterizedTest
    @MethodSource("propertiesArguments")
    public void shouldPassOnDefault(Properties properties) {

        ComposeHttpStatusCodeCheckerConfig checkerConfig = prepareCheckerConfig(properties);

        codeChecker = new ComposeHttpStatusCodeChecker(checkerConfig);

        assertAll(
                () -> {
                    assertThat(codeChecker.isErrorCode(100)).isFalse();
                    assertThat(codeChecker.isErrorCode(200)).isFalse();
                    assertThat(codeChecker.isErrorCode(500)).isTrue();
                    assertThat(codeChecker.isErrorCode(501)).isTrue();
                    assertThat(codeChecker.isErrorCode(400)).isTrue();
                    assertThat(codeChecker.isErrorCode(404)).isTrue();
                });
    }

    @Test
    public void shouldParseIncludeList() {

        Properties properties = new Properties();
        properties.setProperty(
                HttpConnectorConfigConstants.HTTP_ERROR_SINK_CODE_INCLUDE_LIST, STRING_CODES);
        properties.setProperty(
                HttpConnectorConfigConstants.HTTP_ERROR_SINK_CODES_LIST, "1XX, 2XX, 3XX, 4XX, 5XX");

        ComposeHttpStatusCodeCheckerConfig checkerConfig = prepareCheckerConfig(properties);

        codeChecker = new ComposeHttpStatusCodeChecker(checkerConfig);

        assertAll(
                () -> {
                    CODES.forEach(code -> assertThat(codeChecker.isErrorCode(code)).isFalse());

                    assertThat(codeChecker.isErrorCode(301))
                            .withFailMessage(
                                    "Not in include list but matches 3XX range. "
                                            + "Should be considered as error code.")
                            .isTrue();
                });
    }

    @Test
    public void shouldParseErrorCodeList() {

        Properties properties = new Properties();
        properties.setProperty(
                HttpConnectorConfigConstants.HTTP_ERROR_SINK_CODES_LIST, STRING_CODES);

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

        assertAll(
                () -> {
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
        properties.setProperty(HttpConnectorConfigConstants.HTTP_ERROR_SINK_CODES_LIST, listCode);

        ComposeHttpStatusCodeCheckerConfig checkerConfig = prepareCheckerConfig(properties);

        assertThrows(Exception.class, () -> new ComposeHttpStatusCodeChecker(checkerConfig));
    }

    private static Properties prepareErrorCodeProperties(String errorCodeList, String includeList) {
        Properties properties = new Properties();
        properties.setProperty(
                HttpConnectorConfigConstants.HTTP_ERROR_SINK_CODE_INCLUDE_LIST, includeList);
        properties.setProperty(
                HttpConnectorConfigConstants.HTTP_ERROR_SINK_CODES_LIST, errorCodeList);
        return properties;
    }

    private ComposeHttpStatusCodeCheckerConfig prepareCheckerConfig(Properties properties) {
        return ComposeHttpStatusCodeCheckerConfig.builder()
                .properties(properties)
                .includeListPrefix(HttpConnectorConfigConstants.HTTP_ERROR_SINK_CODE_INCLUDE_LIST)
                .errorCodePrefix(HttpConnectorConfigConstants.HTTP_ERROR_SINK_CODES_LIST)
                .build();
    }
}
