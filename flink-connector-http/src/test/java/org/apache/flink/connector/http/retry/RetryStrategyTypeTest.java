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

package org.apache.flink.connector.http.retry;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Test for {@link RetryStrategyType}. */
class RetryStrategyTypeTest {

    static Stream<Arguments> inputArguments() {
        return Stream.of(
                Arguments.of("FIXED-DELAY", RetryStrategyType.FIXED_DELAY),
                Arguments.of("fixed-delay", RetryStrategyType.FIXED_DELAY),
                Arguments.of("exponential-delay", RetryStrategyType.EXPONENTIAL_DELAY),
                Arguments.of("EXPONENTIAL-DELAY", RetryStrategyType.EXPONENTIAL_DELAY));
    }

    @ParameterizedTest
    @MethodSource("inputArguments")
    void parseFromCodes(String code, RetryStrategyType expectedType) {
        var result = RetryStrategyType.fromCode(code);

        assertEquals(expectedType, result);
    }

    @ParameterizedTest
    @ValueSource(strings = {"fixed_delay", "FIXED_DELAY", "ABC", "FIXED-DELA", "exponential_delay"})
    void failWhenCodeIsIllegal(String code) {
        assertThrows(IllegalArgumentException.class, () -> RetryStrategyType.fromCode(code));
    }

    @Test
    void failWhenCodeIsNull() {
        assertThrows(NullPointerException.class, () -> RetryStrategyType.fromCode(null));
    }
}
