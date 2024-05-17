package com.getindata.connectors.http.internal.table.lookup;

import java.net.URLEncoder;
import java.util.*;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.junit.jupiter.api.Test;
import static org.apache.flink.table.factories.utils.FactoryMocks.createTableSource;
import static org.assertj.core.api.Assertions.*;
import static org.junit.jupiter.api.Assertions.assertFalse;

public class HttpLookupTableSourceFactoryTest {

    private static final ResolvedSchema SCHEMA =
        new ResolvedSchema(
            Arrays.asList(
                Column.physical("id", DataTypes.STRING().notNull()),
                Column.physical("msg", DataTypes.STRING().notNull()),
                Column.physical("uuid", DataTypes.STRING().notNull()),
                Column.physical("details", DataTypes.ROW(
                    DataTypes.FIELD("isActive", DataTypes.BOOLEAN()),
                    DataTypes.FIELD("nestedDetails", DataTypes.ROW(
                            DataTypes.FIELD("balance", DataTypes.STRING())
                        )
                    )
                ).notNull())
            ),
            Collections.emptyList(),
            UniqueConstraint.primaryKey("id", List.of("id"))
        );
    @Test
    void validateHttpSourceOptions() {

        HttpLookupTableSourceFactory httpLookupTableSourceFactory
                = new HttpLookupTableSourceFactory();
        TableConfig tableConfig  = new TableConfig();
        httpLookupTableSourceFactory.validateHttpSourceOptions(tableConfig);
        tableConfig.set(HttpLookupConnectorOptions
                .SOURCE_LOOKUP_OIDC_AUTH_TOKEN_REQUEST.key(), "bbb");
        try {
            httpLookupTableSourceFactory.validateHttpSourceOptions(tableConfig);
            assertFalse(true, "Expected an error json processing error");
        } catch (IllegalArgumentException e) {
            // expected
        }

        String json = "{}";
        String urlencoded = URLEncoder.encode(json);
        tableConfig.set(HttpLookupConnectorOptions.SOURCE_LOOKUP_OIDC_AUTH_TOKEN_REQUEST.key(),
                urlencoded);
        try {
            httpLookupTableSourceFactory.validateHttpSourceOptions(tableConfig);
            assertFalse(true, "Expected an error as no grant-type.");
        } catch (IllegalArgumentException e) {
            // expected
        }
        json = "{\"grant-type\":\"password\"}";
        urlencoded = URLEncoder.encode(json);
        tableConfig.set(HttpLookupConnectorOptions.SOURCE_LOOKUP_OIDC_AUTH_TOKEN_REQUEST.key(),
                urlencoded);
        httpLookupTableSourceFactory.validateHttpSourceOptions(tableConfig);
        json = "{\"grant-type\":\"test1\",\"parm\":\"testval\"}";
        urlencoded = URLEncoder.encode(json);
        tableConfig.set(HttpLookupConnectorOptions.SOURCE_LOOKUP_OIDC_AUTH_TOKEN_REQUEST.key(),
                urlencoded);
        httpLookupTableSourceFactory.validateHttpSourceOptions(tableConfig);
    }

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
        return Map.of(
            "connector", "rest-lookup",
            "url", "http://localhost:8080/service",
            "format", "json");
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
