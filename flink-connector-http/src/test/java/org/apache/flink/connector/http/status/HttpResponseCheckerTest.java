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

package org.apache.flink.connector.http.status;

import org.apache.flink.util.ConfigurationException;

import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.net.http.HttpResponse;
import java.util.Set;
import java.util.stream.Stream;

import static java.util.Collections.emptySet;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Test for {@link HttpResponseChecker }. */
class HttpResponseCheckerTest {

    @Test
    void failWhenTheSameCodeIsMarkedSuccessAndError() {
        assertThrows(
                ConfigurationException.class,
                () -> new HttpResponseChecker(Set.of(404), Set.of(404)));
    }

    @Test
    void failWhenSuccessListIsEmpty() {
        assertThrows(
                ConfigurationException.class,
                () -> new HttpResponseChecker(emptySet(), Set.of(500)));
    }

    private static Stream<InputArgs> testData() {
        return Stream.of(
                new InputArgs(404, CodeType.SUCCESSFUL),
                new InputArgs(200, CodeType.SUCCESSFUL),
                new InputArgs(400, CodeType.TEMPORAL_ERROR),
                new InputArgs(408, CodeType.TEMPORAL_ERROR),
                new InputArgs(501, CodeType.TEMPORAL_ERROR),
                new InputArgs(501, CodeType.TEMPORAL_ERROR),
                new InputArgs(502, CodeType.TEMPORAL_ERROR),
                new InputArgs(202, CodeType.ERROR),
                new InputArgs(409, CodeType.ERROR),
                new InputArgs(100, CodeType.ERROR),
                new InputArgs(301, CodeType.ERROR));
    }

    @ParameterizedTest
    @MethodSource("testData")
    void verifyCodes(InputArgs inputArgs) throws ConfigurationException {
        var checker = new HttpResponseChecker("2XX,404,!202", "4XX,!404,500,501,502,!409");
        var response = inputArgs.getResponse();

        switch (inputArgs.getCodeType()) {
            case SUCCESSFUL:
                assertSuccessful(checker, response);
                break;
            case TEMPORAL_ERROR:
                assertTemporalError(checker, response);
                break;
            case ERROR:
                assertError(checker, response);
                break;
            default:
                throw new UnsupportedOperationException();
        }
    }

    private void assertSuccessful(HttpResponseChecker checker, HttpResponse<?> response) {
        assertTrue(checker.isSuccessful(response));
        assertFalse(checker.isTemporalError(response));
    }

    private void assertTemporalError(HttpResponseChecker checker, HttpResponse<?> response) {
        assertFalse(checker.isSuccessful(response));
        assertTrue(checker.isTemporalError(response));
    }

    private void assertError(HttpResponseChecker checker, HttpResponse<?> response) {
        assertFalse(checker.isSuccessful(response));
        assertFalse(checker.isTemporalError(response));
    }

    @RequiredArgsConstructor
    @Getter
    private static class InputArgs {
        @NonNull private final Integer code;
        @NonNull private final CodeType codeType;

        HttpResponse<?> getResponse() {
            var response = mock(HttpResponse.class);
            when(response.statusCode()).thenReturn(code);
            return response;
        }
    }

    private enum CodeType {
        SUCCESSFUL,
        TEMPORAL_ERROR,
        ERROR
    }
}
