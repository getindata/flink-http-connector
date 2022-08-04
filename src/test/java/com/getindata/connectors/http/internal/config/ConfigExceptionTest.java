package com.getindata.connectors.http.internal.config;

import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;

class ConfigExceptionTest {

    @Test
    public void testTemplateMessageWithNull() {
        ConfigException exception = new ConfigException("myProp", -1, null);
        assertThat(exception.getMessage()).isEqualTo("Invalid value -1 for configuration myProp");
    }

    @Test
    public void testTemplateMessage() {
        ConfigException exception = new ConfigException("myProp", -1, "Invalid test value.");
        assertThat(exception.getMessage())
            .isEqualTo("Invalid value -1 for configuration myProp: Invalid test value.");
    }
}
