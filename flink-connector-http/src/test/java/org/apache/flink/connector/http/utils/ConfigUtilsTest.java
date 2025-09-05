/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.http.utils;

import org.apache.flink.connector.http.WireMockServerPortAllocator;
import org.apache.flink.connector.http.config.ConfigException;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.net.Authenticator;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.PasswordAuthentication;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

import static org.apache.flink.connector.http.TestHelper.assertPropertyArray;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ConfigUtilsTest {
    public static final int PORT = WireMockServerPortAllocator.getServerPort();
    public static final int PROXY_PORT = WireMockServerPortAllocator.getServerPort();

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
                .containsAllEntriesOf(
                        Map.of(
                                "my.property", "val2",
                                "my.property.detail", "val4",
                                "my.property.extra", "val5"));
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
    public void shouldGetProxyConfigWithAuthenticator() throws UnknownHostException {
        String proxyHost = "proxy";
        Integer proxyPort = PROXY_PORT;
        Optional<String> proxyUsername = Optional.of("username");
        Optional<String> proxyPassword = Optional.of("password");

        ProxyConfig proxyConfig =
                new ProxyConfig(proxyHost, proxyPort, proxyUsername, proxyPassword);
        assertThat(proxyConfig.getHost().equals("proxy"));
        assertThat(proxyConfig.getAuthenticator().isPresent());

        PasswordAuthentication auth =
                proxyConfig
                        .getAuthenticator()
                        .orElseGet(null)
                        .requestPasswordAuthenticationInstance(
                                "proxy", // host
                                InetAddress.getByName("127.0.0.1"), // address
                                PROXY_PORT, // port
                                "http", // protocol
                                "Please authenticate", // prompt
                                "basic", // scheme
                                null, // URL
                                Authenticator.RequestorType.SERVER // Requestor type
                                );

        PasswordAuthentication auth2 =
                proxyConfig
                        .getAuthenticator()
                        .orElseGet(null)
                        .requestPasswordAuthenticationInstance(
                                "proxy", // host
                                InetAddress.getByName("127.0.0.1"), // address
                                PROXY_PORT, // port
                                "http", // protocol
                                "Please authenticate", // prompt
                                "basic", // scheme
                                null, // URL
                                Authenticator.RequestorType.PROXY // Requestor type
                                );

        assertThat(auth).isNull();
        assertThat(auth2).isNotNull();
        assertThat(auth2.getUserName().equals("username")).isTrue();
        assertThat(Arrays.equals(auth2.getPassword(), "password".toCharArray())).isTrue();
    }

    @Test
    public void shouldGetProxyConfigWithAuthenticatorServer() throws UnknownHostException {
        String proxyHost = "proxy";
        Integer proxyPort = PORT;
        Optional<String> proxyUsername = Optional.of("username");
        Optional<String> proxyPassword = Optional.of("password");

        ProxyConfig proxyConfig =
                new ProxyConfig(proxyHost, proxyPort, proxyUsername, proxyPassword);
        assertThat(proxyConfig.getHost().equals("proxy")).isTrue();
        assertThat(proxyConfig.getAuthenticator().isPresent()).isTrue();

        PasswordAuthentication auth =
                proxyConfig
                        .getAuthenticator()
                        .orElseGet(null)
                        .requestPasswordAuthenticationInstance(
                                "proxy", // host
                                InetAddress.getByName("127.0.0.1"), // address
                                PORT, // port
                                "http", // protocol
                                "Please authenticate", // prompt
                                "basic", // scheme
                                null, // URL
                                Authenticator.RequestorType.SERVER // Requestor type
                                );

        PasswordAuthentication auth2 =
                proxyConfig
                        .getAuthenticator()
                        .orElseGet(null)
                        .requestPasswordAuthenticationInstance(
                                "proxy", // host
                                InetAddress.getByName("127.0.0.1"), // address
                                PORT, // port
                                "http", // protocol
                                "Please authenticate", // prompt
                                "basic", // scheme
                                null, // URL
                                Authenticator.RequestorType.PROXY // Requestor type
                                );

        assertThat(auth).isNull();
        assertThat(auth2).isNotNull();
    }

    @Test
    public void shouldGetProxyConfigWithAuthenticatorWrongHost() throws UnknownHostException {
        String proxyHost = "proxy";
        Integer proxyPort = PORT;
        Optional<String> proxyUsername = Optional.of("username");
        Optional<String> proxyPassword = Optional.of("password");

        ProxyConfig proxyConfig =
                new ProxyConfig(proxyHost, proxyPort, proxyUsername, proxyPassword);
        assertThat(proxyConfig.getHost().equals("proxy")).isTrue();
        assertThat(proxyConfig.getAuthenticator().isPresent()).isTrue();

        PasswordAuthentication auth =
                proxyConfig
                        .getAuthenticator()
                        .get()
                        .requestPasswordAuthenticationInstance(
                                "wrong", // host
                                InetAddress.getByName("127.0.0.1"), // address
                                PORT, // port
                                "http", // protocol
                                "Please authenticate", // prompt
                                "basic", // scheme
                                null, // URL
                                Authenticator.RequestorType.PROXY // Requestor type
                                );

        PasswordAuthentication auth2 =
                proxyConfig
                        .getAuthenticator()
                        .orElseGet(null)
                        .requestPasswordAuthenticationInstance(
                                "proxy", // host
                                InetAddress.getByName("127.0.0.1"), // address
                                PORT, // port
                                "http", // protocol
                                "Please authenticate", // prompt
                                "basic", // scheme
                                null, // URL
                                Authenticator.RequestorType.PROXY // Requestor type
                                );

        assertThat(auth).isNull();
        assertThat(auth2).isNotNull();
    }

    @Test
    public void shouldGetProxyConfigWithoutAuthenticator()
            throws MalformedURLException, UnknownHostException {
        String proxyHost = "proxy";
        Optional<String> proxyUsername = Optional.of("username");
        Optional<String> proxyPassword = Optional.empty();

        ProxyConfig proxyConfig = new ProxyConfig(proxyHost, 80, proxyUsername, proxyPassword);
        assertThat(proxyConfig.getHost().equals("proxy")).isTrue();
        assertThat(proxyConfig.getAuthenticator().isEmpty()).isTrue();
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

        // should throw on non String key regardless of key prefix.
        Properties nonStringProperties = new Properties();
        nonStringProperties.put(new Object(), 1);
        assertThatThrownBy(
                        () ->
                                ConfigUtils.propertiesToMap(
                                        nonStringProperties, "a.property", String.class))
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

        assertThatThrownBy(() -> ConfigUtils.extractPropertyLastElement(invalidProperty))
                .isInstanceOf(ConfigException.class);
    }

    @Test
    public void flatMapPropertyMap() {
        Map<String, String> propertyMap =
                Map.of(
                        "propertyOne", "val1",
                        "propertyTwo", "val2",
                        "propertyThree", "val3");

        String[] propertyArray = HttpHeaderUtils.toHeaderAndValueArray(propertyMap);

        // size is == propertyMap.key size + propertyMap.value.size
        assertThat(propertyArray).hasSize(6);

        // assert that we have property followed by its value.
        assertPropertyArray(propertyArray, "propertyOne", "val1");
        assertPropertyArray(propertyArray, "propertyTwo", "val2");
        assertPropertyArray(propertyArray, "propertyThree", "val3");
    }
}
