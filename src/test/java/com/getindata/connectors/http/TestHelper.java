package com.getindata.connectors.http;

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

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

}
