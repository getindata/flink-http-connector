package com.getindata.connectors.http.internal.status;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.Singular;
import org.apache.flink.util.ConfigurationException;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class HttpCodesParserTest {

    @ParameterizedTest
    @ValueSource(strings = {
        "6XX",
        "1XXX",
        "600",
        "99",
        "1XX,11",
        "abc",
        "!1XX",
        "1 2 3",
        "1X X"
    })
    void failWhenCodeExpressionIsInvalid(String codeExpression) {
        assertThrows(ConfigurationException.class,
            () -> HttpCodesParser.parse(codeExpression));
    }

    private static Stream<InputArgs> inputArgsStream() {
        return Stream.of(
                InputArgs.builder().codeExpression("2XX,404,!203,!205")
                        .expectedCodes(range(200, 300, 203, 205))
                        .expectedCode(404)
                        .build(),
                InputArgs.builder().codeExpression("  400, 401 , 403, 500,501,  !502")
                        .expectedCodes(List.of(400, 401, 403, 500, 501))
                        .build(),
                InputArgs.builder().codeExpression("!405,1XX,  2XX ,404,!202,405")
                        .expectedCodes(range(100, 300, 202))
                        .expectedCode(404)
                        .build(),
                InputArgs.builder().codeExpression("!404, 4XX")
                        .expectedCodes(range(400, 500, 404))
                        .build(),
                InputArgs.builder().codeExpression("2xX,!401,3Xx,4xx")
                        .expectedCodes(range(200, 500, 401))
                        .build()
        );
    }

    @ParameterizedTest
    @MethodSource("inputArgsStream")
    void parseCodeExpressionTest(InputArgs inputArgs) throws ConfigurationException {
        var expectedCodes = inputArgs.getExpectedCodes();

        var result = HttpCodesParser.parse(inputArgs.getCodeExpression());

        for (var code : expectedCodes) {
            assertTrue(result.contains(code), "Missing code " + code);
        }
        for (var code : result) {
            assertTrue(expectedCodes.contains(code), "Improper code " + code);
        }
    }

    private static List<Integer> range(int start, int endExclusive, int... exclusions) {
        var exclusionSet = Arrays.stream(exclusions).boxed().collect(Collectors.toSet());
        return IntStream.range(start, endExclusive).boxed()
                .filter(item -> !exclusionSet.contains(item))
                .collect(Collectors.toList());
    }

    @Builder
    @Getter
    private static class InputArgs {
        @NonNull
        private final String codeExpression;
        @Singular
        private final Set<Integer> expectedCodes;
    }
}
