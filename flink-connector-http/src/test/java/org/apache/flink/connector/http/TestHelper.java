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

package org.apache.flink.connector.http;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.junit.jupiter.api.Assertions;

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Objects;

import static org.assertj.core.api.Assertions.assertThat;

/** Test helper. */
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
            String[] headerArray, String propertyName, String expectedValue) {
        // important thing is that we have property followed by its value.
        for (int i = 0; i < headerArray.length; i++) {
            if (headerArray[i].equals(propertyName)) {
                assertThat(headerArray[i + 1])
                        .withFailMessage(
                                "Property Array does not contain property name, value pairs.")
                        .isEqualTo(expectedValue);
                return;
            }
        }
        Assertions.fail(
                String.format(
                        "Missing property name [%s] in header array %s.",
                        propertyName, Arrays.toString(headerArray)));
    }
}
