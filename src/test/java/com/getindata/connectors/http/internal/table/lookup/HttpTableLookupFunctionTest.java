package com.getindata.connectors.http.internal.table.lookup;

import java.util.Map;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;

import static com.getindata.connectors.http.internal.table.lookup.HttpLookupConnectorOptions.SOURCE_LOOKUP_FAIL_JOB_ON_ERROR;

public class HttpTableLookupFunctionTest {
    @Test
    void testconstructor() {
        assertThat(getHttpTableLookupFunction(null).getFail_job_on_error()).isTrue();
        assertThat(getHttpTableLookupFunction(true).getFail_job_on_error()).isTrue();
        assertThat(getHttpTableLookupFunction(false).getFail_job_on_error()).isFalse();
    }
    @Test
    private static @NotNull HttpTableLookupFunction getHttpTableLookupFunction(Boolean flag) {
        ReadableConfig readableConfig;
        if (flag == null ) {
            readableConfig = new Configuration();
        } else {
            Map<String, String> optionsMap = Map.of(SOURCE_LOOKUP_FAIL_JOB_ON_ERROR.key(),
                    Boolean.valueOf(flag).toString());
            readableConfig=Configuration.fromMap(optionsMap);
        }
        HttpLookupConfig httpLookupConfig = HttpLookupConfig.builder()
                .readableConfig(readableConfig)
                .build();
        HttpTableLookupFunction  httpTableLookupFunction
                = new HttpTableLookupFunction(null,
                null,
                null,
                httpLookupConfig,
                null,
                null);
        return httpTableLookupFunction;
    }

}
