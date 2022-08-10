package com.getindata.connectors.http;

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Objects;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.junit.jupiter.api.Assertions;
import static org.assertj.core.api.Assertions.assertThat;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class TestHelper {

    private static final TestHelper INSTANCE = new TestHelper();

    public static String readTestFile(String pathToFile) {
        try {
            URI uri = Objects.requireNonNull(INSTANCE.getClass().getResource(pathToFile)).toURI();
            return Files.readString(Path.of(uri));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static void assertPropertyArray(
            String[] headerArray,
            String propertyName,
            String expectedValue) {
        // important thing is that we have property followed by its value.
        for (int i = 0; i < headerArray.length; i++) {
            if (headerArray[i].equals(propertyName)) {
                assertThat(headerArray[i + 1])
                    .withFailMessage("Property Array does not contain property name, value pairs.")
                    .isEqualTo(expectedValue);
                return;
            }
        }
        Assertions.fail(
            String.format(
                "Missing property name [%s] in header array %s.",
                propertyName,
                Arrays.toString(headerArray))
        );
    }
}
