package com.getindata.connectors.http.internal.utils.uri;

import java.util.stream.Stream;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertThrows;

class CharArrayBufferTest {

    @Test
    public void testInvalidCapacity() {
        assertThrows(IllegalArgumentException.class, () -> new CharArrayBuffer(0));
    }

    @Test
    public void testExpandCapacity() {
        String testText = "Hello My Friend";

        CharArrayBuffer charArrayBuffer = new CharArrayBuffer(1);
        charArrayBuffer.append(testText);

        assertThat(charArrayBuffer.length()).isEqualTo(testText.length());
    }

    @Test
    public void testSubSequence() {
        String testText = "Hello My Friend";

        CharArrayBuffer charArrayBuffer = new CharArrayBuffer(1);
        charArrayBuffer.append(testText);

        assertAll(() -> {
                Assertions.assertThrows(IndexOutOfBoundsException.class,
                    () -> charArrayBuffer.subSequence(-1, 1));
                Assertions.assertThrows(IndexOutOfBoundsException.class,
                    () -> charArrayBuffer.subSequence(1, -1));
            Assertions.assertThrows(IndexOutOfBoundsException.class,
                () -> charArrayBuffer.subSequence(2, 1));
            Assertions.assertThrows(IndexOutOfBoundsException.class,
                () -> charArrayBuffer.subSequence(2, testText.length() + 5));
            assertThat(charArrayBuffer.subSequence(2, 10).toString()).isEqualTo("llo My Fri");
            }
        );
    }

    private static Stream<Arguments> appendArgs() {
        return Stream.of(
          Arguments.of("", "baseString"),
          Arguments.of(" ", "baseString "),
          Arguments.of(null, "baseStringnull")
        );
    }

    @ParameterizedTest
    @MethodSource("appendArgs")
    public void testAppend(String stringToAppend, String expected) {
        CharArrayBuffer charArrayBuffer = new CharArrayBuffer(1);
        charArrayBuffer.append("baseString");

        assertAll(() -> {
                assertThat(charArrayBuffer.toString()).isEqualTo("baseString");
                charArrayBuffer.append(stringToAppend);
                assertThat(charArrayBuffer.toString()).isEqualTo(expected);
            }
        );
    }
}
