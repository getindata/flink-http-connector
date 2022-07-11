package com.getindata.connectors.http;

import java.util.List;
import java.util.Map;

import org.apache.flink.table.data.RowData;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;

import com.getindata.connectors.http.internal.JsonResultTableConverter;
import com.getindata.connectors.http.internal.JsonResultTableConverter.HttpResultConverterOptions;
import static com.getindata.connectors.http.TestHelper.readTestFile;

class JsonResultTableConverterImplTest {

    private static final String SAMPLES_FOLDER = "/http/";

    private JsonResultTableConverter converter;

    private HttpResultConverterOptions converterOptions;

    private String json;

    @BeforeEach
    public void setUp() {
        json = readTestFile(SAMPLES_FOLDER + "HttpResult.json");
    }

    @Test
    void shouldHandleMissingField() {
        converterOptions =
            HttpResultConverterOptions.builder().columnNames(List.of("missingField")).build();

        converter = new JsonResultTableConverter(converterOptions);
        RowData rowData = converter.convert(json);

        String value = rowData.getString(0).toString();
        assertThat(value).isEmpty();
    }

    @Test
    void shouldFindSimpleValue() {
        converterOptions = HttpResultConverterOptions.builder().columnNames(List.of("msg")).build();

        converter = new JsonResultTableConverter(converterOptions);
        RowData rowData = converter.convert(json);

        String value = rowData.getString(0).toString();
        assertThat(value).isEqualTo("Returned HTTP message for parameter PARAM, COUNTER");
    }

    @Test
    void shouldHandleInvalidJson() {
        List<String> columnNames = List.of("msg", "id");
        converterOptions = HttpResultConverterOptions.builder().columnNames(columnNames).build();

        converter = new JsonResultTableConverter(converterOptions);
        RowData rowData = converter.convert("{Invalid Json}");
        assertThat(rowData.getArity()).isEqualTo(columnNames.size());
    }

    @Test
    void shouldFindNestedValues() {
        converterOptions =
            HttpResultConverterOptions.builder()
                .columnNames(List.of("isActive", "balance"))
                .aliases(
                    Map.of(
                        "isActive", "$.details.isActive",
                        "balance", "$.details.nestedDetails.balance"))
                .build();

        converter = new JsonResultTableConverter(converterOptions);
        RowData rowData = converter.convert(json);

        String isActive = rowData.getString(0).toString();
        assertThat(isActive).isEqualTo("true");

        String balance = rowData.getString(1).toString();
        assertThat(balance).isEqualTo("$1,729.34");
    }

    @Test
    void shouldFindNestedValuesWIthMissingJsonPathHeader() {
        converterOptions =
            HttpResultConverterOptions.builder()
                .columnNames(List.of("isActive"))
                .aliases(Map.of("isActive", "details.isActive"))
                .build();

        converter = new JsonResultTableConverter(converterOptions);
        RowData rowData = converter.convert(json);

        String isActive = rowData.getString(0).toString();
        assertThat(isActive).isEqualTo("true");
    }

    @Test
    void shouldFindValueWithRoot() {
        converterOptions =
            HttpResultConverterOptions.builder()
                .columnNames(List.of("isActive"))
                .root("$.details.")
                .build();

        converter = new JsonResultTableConverter(converterOptions);
        RowData rowData = converter.convert(json);

        String value = rowData.getString(0).toString();
        assertThat(value).isEqualTo("true");
    }

}
