package com.getindata.connectors.http.internal.utils;
import java.time.Duration;

import org.apache.flink.configuration.Configuration;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import com.getindata.connectors.http.internal.HeaderPreprocessor;
import static com.getindata.connectors.http.internal.table.lookup.HttpLookupConnectorOptions.*;



public class HttpHeaderUtilsTest {
    @Test
    void shouldCreateOIDCHeaderPreprocessorTest() {
        Configuration configuration = new Configuration();
        HeaderPreprocessor headerPreprocessor
                = HttpHeaderUtils.createOIDCHeaderPreprocessor(configuration);
        assertThat(headerPreprocessor).isNull();
        configuration.setString(SOURCE_LOOKUP_OIDC_AUTH_TOKEN_ENDPOINT_URL.key(), "http://aaa");
        configuration.setString(SOURCE_LOOKUP_OIDC_AUTH_TOKEN_REQUEST.key(), "ccc");
        configuration.set(SOURCE_LOOKUP_OIDC_AUTH_TOKEN_EXPIRY_REDUCTION, Duration.ofSeconds(1));
        headerPreprocessor
                = HttpHeaderUtils.createOIDCHeaderPreprocessor(configuration);
        assertThat(headerPreprocessor).isNotNull();
    }
}
