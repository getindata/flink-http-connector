package com.getindata.connectors.http.internal.utils;

import java.util.Map;
import java.util.Properties;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.getindata.connectors.http.internal.config.ConfigException;

class ConfigUtilsTest {

    @Test
    public void shouldExtractPropertiesToMap() {
        Properties properties = new Properties();
        properties.setProperty("property", "val1");
        properties.setProperty("my.property", "val2");
        properties.setProperty("my.super.property", "val3");
        properties.setProperty("my.property.detail", "val4");
        properties.setProperty("my.property.extra", "val5");
        properties.setProperty("another.my.property.extra", "val6");

        Map<String, String> mappedProperties =
            ConfigUtils.propertiesToMap(properties, "my.property", String.class);

        assertThat(mappedProperties).hasSize(3);
        assertThat(mappedProperties)
            .containsExactlyEntriesOf(
                Map.of(
                    "my.property", "val2",
                    "my.property.detail", "val4",
                    "my.property.extra", "val5"
                ));
    }

    @Test
    public void shouldConvertNoProperty() {
        Properties properties = new Properties();
        properties.setProperty("property", "val1");
        properties.setProperty("my.property", "val2");
        properties.setProperty("my.super.property", "val3");

        Map<String, String> mappedProperties =
            ConfigUtils.propertiesToMap(properties, "my.custom", String.class);
        assertThat(mappedProperties).isEmpty();
    }

    @Test
    public void shouldHandleInvalidPropertyType() {

        Properties properties = new Properties();
        properties.put("a.property", 1);

        // Should ignore "invalid" property since does not match the prefix
        Map<String, String> mappedProperties =
            ConfigUtils.propertiesToMap(properties, "my.custom", String.class);
        assertThat(mappedProperties).isEmpty();

        // should throw on invalid value, when name matches the prefix.
        assertThatThrownBy(
            () -> ConfigUtils.propertiesToMap(properties, "a.property", String.class))
            .isInstanceOf(ConfigException.class);
    }

    @ParameterizedTest(name = "Property full name - {0}")
    @ValueSource(strings = {"property", "my.property", "my.super.property", ".my.super.property"})
    public void shouldGetPropertyName(String fullPropertyName) {

        String propertyLastElement = ConfigUtils.extractPropertyLastElement(fullPropertyName);
        assertThat(propertyLastElement).isEqualTo("property");
    }

    @ParameterizedTest(name = "Property full name - {0}")
    @ValueSource(strings = {"", " ", "my.super.property.", ".", "..."})
    public void shouldThrowOnInvalidProperty(String invalidProperty) {

        assertThatThrownBy(
            () -> ConfigUtils.extractPropertyLastElement(invalidProperty))
            .isInstanceOf(ConfigException.class);
    }
}
