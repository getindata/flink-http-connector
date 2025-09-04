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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.http.WireMockServerPortAllocator;
import org.apache.flink.connector.http.table.lookup.HttpLookupConfig;
import org.apache.flink.connector.http.table.lookup.Slf4JHttpLookupPostRequestCallback;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;

import org.junit.jupiter.api.Test;

import java.net.Authenticator;
import java.net.InetAddress;
import java.net.PasswordAuthentication;
import java.net.UnknownHostException;
import java.net.http.HttpClient;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.apache.flink.connector.http.config.HttpConnectorConfigConstants.SOURCE_PROXY_HOST;
import static org.apache.flink.connector.http.config.HttpConnectorConfigConstants.SOURCE_PROXY_PASSWORD;
import static org.apache.flink.connector.http.config.HttpConnectorConfigConstants.SOURCE_PROXY_PORT;
import static org.apache.flink.connector.http.config.HttpConnectorConfigConstants.SOURCE_PROXY_USERNAME;
import static org.assertj.core.api.Assertions.assertThat;

class JavaNetHttpClientFactoryTest {
    public static final int SERVER_PORT = WireMockServerPortAllocator.getServerPort();
    public static final int PROXY_SERVER_PORT = WireMockServerPortAllocator.PORT_BASE - 1;

    @Test
    public void shouldGetClientWithAuthenticator() throws UnknownHostException {
        Properties properties = new Properties();
        Configuration configuration = new Configuration();
        configuration.setString(SOURCE_PROXY_HOST, "google");
        configuration.setString(SOURCE_PROXY_PORT, String.valueOf(PROXY_SERVER_PORT));
        configuration.setString(SOURCE_PROXY_USERNAME, "username");
        configuration.setString(SOURCE_PROXY_PASSWORD, "password");

        HttpLookupConfig lookupConfig =
                HttpLookupConfig.builder()
                        .url("https://google.com")
                        .readableConfig(configuration)
                        .properties(properties)
                        .httpPostRequestCallback(new Slf4JHttpLookupPostRequestCallback())
                        .build();

        HttpClient client = JavaNetHttpClientFactory.createClient(lookupConfig);

        assertThat(client.authenticator().isPresent()).isTrue();
        assertThat(client.proxy().isPresent()).isTrue();

        PasswordAuthentication auth =
                client.authenticator()
                        .get()
                        .requestPasswordAuthenticationInstance(
                                "google", // host
                                InetAddress.getByName("127.0.0.1"), // address
                                SERVER_PORT, // port
                                "http", // protocol
                                "Please authenticate", // prompt
                                "basic", // scheme
                                null, // URL
                                Authenticator.RequestorType.PROXY // Requestor type
                                );

        assertThat(auth.getUserName().equals("username")).isTrue();
        assertThat(Arrays.equals(auth.getPassword(), "password".toCharArray())).isTrue();
    }

    @Test
    public void shouldGetClientWithoutAuthenticator() throws UnknownHostException {
        Properties properties = new Properties();
        Configuration configuration = new Configuration();
        configuration.setString(SOURCE_PROXY_HOST, "google");
        configuration.setString(SOURCE_PROXY_PORT, String.valueOf(PROXY_SERVER_PORT));

        HttpLookupConfig lookupConfig =
                HttpLookupConfig.builder()
                        .url("https://google.com")
                        .readableConfig(configuration)
                        .properties(properties)
                        .httpPostRequestCallback(new Slf4JHttpLookupPostRequestCallback())
                        .build();

        HttpClient client = JavaNetHttpClientFactory.createClient(lookupConfig);

        assertThat(client.authenticator().isEmpty()).isTrue();
        assertThat(client.proxy().isPresent()).isTrue();
    }

    @Test
    public void shouldGetClientWithoutProxy() {
        Properties properties = new Properties();
        Configuration configuration = new Configuration();

        HttpLookupConfig lookupConfig =
                HttpLookupConfig.builder()
                        .url("https://google.com")
                        .readableConfig(configuration)
                        .properties(properties)
                        .httpPostRequestCallback(new Slf4JHttpLookupPostRequestCallback())
                        .build();

        HttpClient client = JavaNetHttpClientFactory.createClient(lookupConfig);
        assertThat(client.authenticator().isEmpty()).isTrue();
        assertThat(client.proxy().isEmpty()).isTrue();
    }

    @Test
    public void shouldGetClientWithExecutor() {
        Properties properties = new Properties();
        ExecutorService httpClientExecutor =
                Executors.newFixedThreadPool(
                        1,
                        new ExecutorThreadFactory(
                                "http-sink-client-batch-request-worker",
                                ThreadUtils.LOGGING_EXCEPTION_HANDLER));

        HttpClient client = JavaNetHttpClientFactory.createClient(properties, httpClientExecutor);
        assertThat(client.followRedirects().equals(HttpClient.Redirect.NORMAL)).isTrue();
    }
}
