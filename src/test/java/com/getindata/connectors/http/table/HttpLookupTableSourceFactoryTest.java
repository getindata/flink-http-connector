package com.getindata.connectors.http.table;

import static org.apache.flink.table.factories.utils.FactoryMocks.createTableSource;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.junit.jupiter.api.Test;

public class HttpLookupTableSourceFactoryTest {

  private static final ResolvedSchema SCHEMA =
      new ResolvedSchema(
          Arrays.asList(
              Column.physical("id", DataTypes.STRING().notNull()),
              Column.physical("msg", DataTypes.STRING().notNull()),
              Column.physical("uuid", DataTypes.STRING().notNull()),
              Column.physical("isActive", DataTypes.STRING().notNull()),
              Column.physical("balance", DataTypes.STRING().notNull())),
          Collections.emptyList(),
          UniqueConstraint.primaryKey("id", List.of("id")));

  @Test
  void shouldCreateForMandatoryFields() {
    Map<String, String> options = getMandatoryOptions();
    DynamicTableSource source = createTableSource(SCHEMA, options);
    assertThat(source).isNotNull();
    assertThat(source).isInstanceOf(HttpLookupTableSource.class);
  }

  @Test
  void shouldThrowIfMissingUrl() {
    Map<String, String> options = Collections.singletonMap("connector", "rest-lookup");
    assertThatExceptionOfType(ValidationException.class)
        .isThrownBy(() -> createTableSource(SCHEMA, options));
  }

  @Test
  void shouldAcceptRootParameter() {
    Map<String, String> options = getOptions(Map.of("root", "$.some.path"));
    DynamicTableSource source = createTableSource(SCHEMA, options);
    assertThat(source).isNotNull();
    assertThat(source).isInstanceOf(HttpLookupTableSource.class);
  }

  @Test
  void shouldAcceptAliasParameter() {
    Map<String, String> options = getOptions(Map.of("field.isActive.path", "$.details.isActive"));
    DynamicTableSource source = createTableSource(SCHEMA, options);
    assertThat(source).isNotNull();
    assertThat(source).isInstanceOf(HttpLookupTableSource.class);
  }

  @Test
  void shouldThrowIfAliasDoesNotMathColumn() {
    Map<String, String> options = getOptions(Map.of("field.bogusField.path", "$.details.isActive"));
    assertThatExceptionOfType(ValidationException.class)
        .isThrownBy(() -> createTableSource(SCHEMA, options));
  }

  @Test
  void shouldAcceptWithUrlArgs() {
    Map<String, String> options = getOptions(Map.of("url-args", "id;msg"));
    DynamicTableSource source = createTableSource(SCHEMA, options);
    assertThat(source).isNotNull();
    assertThat(source).isInstanceOf(HttpLookupTableSource.class);
  }

  @Test
  void shouldHandleEmptyUrlArgs() {
    Map<String, String> options = getOptions(Collections.emptyMap());
    DynamicTableSource source = createTableSource(SCHEMA, options);
    assertThat(source).isNotNull();
    assertThat(source).isInstanceOf(HttpLookupTableSource.class);
  }

  private Map<String, String> getMandatoryOptions() {
    return Map.of("connector", "rest-lookup", "url", "http://localhost:8080/service");
  }

  private Map<String, String> getOptions(Map<String, String> optionalOptions) {
    if (optionalOptions.isEmpty()) {
      return getMandatoryOptions();
    }

    Map<String, String> allOptions = new HashMap<>();
    allOptions.putAll(getMandatoryOptions());
    allOptions.putAll(optionalOptions);

    return allOptions;
  }
}
