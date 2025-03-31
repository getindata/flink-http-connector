package com.getindata.connectors.http.internal.retry;

import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class RetryStrategyTypeTest {

    static Stream<Arguments> inputArguments() {
        return Stream.of(
                Arguments.of("FIXED-DELAY", RetryStrategyType.FIXED_DELAY),
                Arguments.of("fixed-delay", RetryStrategyType.FIXED_DELAY),
                Arguments.of("exponential-delay", RetryStrategyType.EXPONENTIAL_DELAY),
                Arguments.of("EXPONENTIAL-DELAY", RetryStrategyType.EXPONENTIAL_DELAY)
        );
    }

    @ParameterizedTest
    @MethodSource("inputArguments")
    void parseFromCodes(String code, RetryStrategyType expectedType) {
        var result = RetryStrategyType.fromCode(code);

        assertEquals(expectedType, result);
    }

    @ParameterizedTest
    @ValueSource(strings = {
        "fixed_delay",
        "FIXED_DELAY",
        "ABC",
        "FIXED-DELA",
        "exponential_delay"
    })
    void failWhenCodeIsIllegal(String code) {
        assertThrows(IllegalArgumentException.class, () -> RetryStrategyType.fromCode(code));
    }

    @Test
    void failWhenCodeIsNull() {
        assertThrows(NullPointerException.class, () -> RetryStrategyType.fromCode(null));
    }
}
