package com.getindata.connectors.http;

import static com.getindata.connectors.http.table.TableSourceHelper.buildEmptyRow;
import static com.getindata.connectors.http.table.TableSourceHelper.buildGenericRowData;
import static org.apache.commons.lang3.StringUtils.isNotBlank;

import com.getindata.connectors.http.table.TableSourceHelper;
import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.PathNotFoundException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;

@Slf4j
@RequiredArgsConstructor
public class JsonResultTableConverter implements HttpResultConverter {

  private final HttpResultConverterOptions converterOptions;

  @Override
  public RowData convert(String json) {
    try {
      List<Object> rowData = new ArrayList<>();

      List<String> columnNames = converterOptions.getColumnNames();
      for (String columnName : columnNames) {
        String jsonPath = columnName;
        String aliasPath = converterOptions.getAliases().get(columnName);
        if (isNotBlank(aliasPath)) {
          jsonPath = aliasPath;
        }

        String root = converterOptions.getRoot();
        if (isNotBlank(root)) {
          jsonPath = root + jsonPath;
        }

        if (!jsonPath.startsWith("$.")) {
          jsonPath = "$." + jsonPath;
        }

        DocumentContext parsedJson = JsonPath.parse(json);
        Object value = getValueForPath(parsedJson, jsonPath);

        // TODO FIX DO NOT USE ToString HERE. Make some data type converter based on the real type.
        // Also don't blindly convert to StringData, use different types maybe?
        rowData.add(StringData.fromString(value.toString()));
      }

      return buildGenericRowData(rowData);

    } catch (Exception e) {
      log.error("Exception while converting Json.", e);
      return buildEmptyRow(converterOptions.getColumnNames().size());
    }
  }

  private Object getValueForPath(DocumentContext parsedJson, String jsonPath) {
    // TODO Look for Type Maybe?
    try {
      return parsedJson.read(jsonPath);
    } catch (PathNotFoundException e) {
      log.warn("Could not find JsonPath - " + jsonPath);
    }

    return "";
  }

  @Builder
  @Data
  @RequiredArgsConstructor(access = AccessLevel.PRIVATE)
  public static class HttpResultConverterOptions {

    @Builder.Default private final Map<String, String> aliases = new HashMap<>();

    private final String root;

    @Builder.Default private final List<String> columnNames = new ArrayList<>();
  }
}
