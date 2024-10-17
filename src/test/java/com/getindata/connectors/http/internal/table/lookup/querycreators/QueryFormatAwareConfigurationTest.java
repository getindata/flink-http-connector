package com.getindata.connectors.http.internal.table.lookup.querycreators;

import java.util.Collections;
import java.util.Optional;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;

class QueryFormatAwareConfigurationTest {

    private static final ConfigOption<String> configOption = ConfigOptions.key("key")
        .stringType()
        .noDefaultValue();

    @Test
    public void testWithDot() {
        QueryFormatAwareConfiguration queryConfig = new QueryFormatAwareConfiguration(
            "prefix.", Configuration.fromMap(Collections.singletonMap("prefix.key", "val"))
        );

        Optional<String> optional = queryConfig.getOptional(configOption);
        assertThat(optional.get()).isEqualTo("val");
    }

    @Test
    public void testWithoutDot() {
        QueryFormatAwareConfiguration queryConfig = new QueryFormatAwareConfiguration(
            "prefix", Configuration.fromMap(Collections.singletonMap("prefix.key", "val"))
        );

        Optional<String> optional = queryConfig.getOptional(configOption);
        assertThat(optional.get()).isEqualTo("val");
    }

}
