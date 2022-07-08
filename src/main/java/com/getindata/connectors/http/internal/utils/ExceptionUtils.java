package com.getindata.connectors.http.internal.utils;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.UncheckedIOException;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access = AccessLevel.NONE)
public final class ExceptionUtils {

  public static String stringifyException(Throwable e) {
    try (StringWriter stm = new StringWriter();
        PrintWriter wrt = new PrintWriter(stm)) {

      e.printStackTrace(wrt);
      wrt.close();
      return stm.toString();

    } catch (IOException ioException) {
      throw new UncheckedIOException(ioException);
    }
  }
}
