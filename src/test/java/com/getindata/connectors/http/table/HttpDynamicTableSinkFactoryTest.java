package com.getindata.connectors.http.table;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertThrows;
import org.apache.flink.table.api.ValidationException;

// Unfortunately it seems that Flink is lazy with connector instantiation,
// so one has to call INSERT in order to test the Factory
public class HttpDynamicTableSinkFactoryTest {
  protected StreamExecutionEnvironment env;
  protected StreamTableEnvironment tEnv;

  @BeforeEach
  public void setup() {
    env = StreamExecutionEnvironment.getExecutionEnvironment();
    tEnv = StreamTableEnvironment.create(env);
  }

  @Test
  public void requiredOptionsTest() {
    final String noFormatOptionCreate =
        String.format(
            "CREATE TABLE formatHttp (\n"
            + "  id bigint\n"
            + ") with (\n"
            + "  'connector' = '%s',\n"
            + "  'url' = '%s'\n"
            + ")",
            HttpDynamicTableSinkFactory.IDENTIFIER,
            "http://localhost/"
        );
    tEnv.executeSql(noFormatOptionCreate);
    assertThrows(ValidationException.class, () -> tEnv.executeSql("INSERT INTO formatHttp VALUES (1)").await());

    final String noUrlOptionCreate =
        String.format(
            "CREATE TABLE urlHttp (\n"
            + "  id bigint\n"
            + ") with (\n"
            + "  'connector' = '%s',\n"
            + "  'format' = 'json'\n"
            + ")",
            HttpDynamicTableSinkFactory.IDENTIFIER
        );
    tEnv.executeSql(noUrlOptionCreate);
    assertThrows(ValidationException.class, () -> tEnv.executeSql("INSERT INTO urlHttp VALUES (1)").await());
  }

  @Test
  public void validateHttpSinkOptionsTest() {
    final String invalidInsertMethod =
        String.format(
            "CREATE TABLE http (\n"
            + "  id bigint\n"
            + ") with (\n"
            + "  'connector' = '%s',\n"
            + "  'url' = '%s',\n"
            + "  'format' = 'json',\n"
            + "  'insert-method' = 'GET'\n"
            + ")",
            HttpDynamicTableSinkFactory.IDENTIFIER,
            "http://localhost/"
        );
    tEnv.executeSql(invalidInsertMethod);
    assertThrows(ValidationException.class, () -> tEnv.executeSql("INSERT INTO http VALUES (1)").await());
  }

  @Test
  public void nonexistentOptionsTest() {
    final String invalidInsertMethod =
        String.format(
            "CREATE TABLE http (\n"
            + "  id bigint\n"
            + ") with (\n"
            + "  'connector' = '%s',\n"
            + "  'url' = '%s',\n"
            + "  'format' = 'json',\n"
            + "  'some-random-totally-unexisting-option-!g*Av#' = '7123'\n"
            + ")",
            HttpDynamicTableSinkFactory.IDENTIFIER,
            "http://localhost/"
        );
    tEnv.executeSql(invalidInsertMethod);
    assertThrows(ValidationException.class, () -> tEnv.executeSql("INSERT INTO http VALUES (1)").await());
  }
}
