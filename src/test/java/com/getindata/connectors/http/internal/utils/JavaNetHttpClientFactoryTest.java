package com.getindata.connectors.http.internal.utils;

import java.net.http.HttpClient;
import java.util.Properties;

import org.apache.flink.configuration.Configuration;
import org.junit.jupiter.api.Test;

import com.getindata.connectors.http.internal.table.lookup.HttpLookupConfig;
import com.getindata.connectors.http.internal.table.lookup.Slf4JHttpLookupPostRequestCallback;
import static com.getindata.connectors.http.internal.config.HttpConnectorConfigConstants.*;

class JavaNetHttpClientFactoryTest {

    @Test
    public void shouldGetClientWithAuthenticator() {
        Properties properties = new Properties();
        Configuration configuration = new Configuration();
        configuration.setString(SOURCE_PROXY_HOST, "google");
        configuration.setString(SOURCE_PROXY_PORT, "8080");
        configuration.setString(SOURCE_PROXY_USERNAME, "username");
        configuration.setString(SOURCE_PROXY_PASSWORD, "password");

        HttpLookupConfig lookupConfig = HttpLookupConfig.builder()
                .url("https://google.com")
                .readableConfig(configuration)
                .properties(properties)
                .httpPostRequestCallback(new Slf4JHttpLookupPostRequestCallback())
                .build();

        HttpClient client = JavaNetHttpClientFactory.createClient(lookupConfig);

        assert(client.authenticator().isPresent());
        assert(client.proxy().isPresent());
    }

    @Test
    public void shouldGetClientWithoutAuthenticator() {
        Properties properties = new Properties();
        Configuration configuration = new Configuration();
        configuration.setString(SOURCE_PROXY_HOST, "google");
        configuration.setString(SOURCE_PROXY_PORT, "8080");

        HttpLookupConfig lookupConfig = HttpLookupConfig.builder()
                .url("https://google.com")
                .readableConfig(configuration)
                .properties(properties)
                .httpPostRequestCallback(new Slf4JHttpLookupPostRequestCallback())
                .build();

        HttpClient client = JavaNetHttpClientFactory.createClient(lookupConfig);

        assert(client.authenticator().isEmpty());
        assert(client.proxy().isPresent());
    }

    @Test
    public void shouldGetClientWithoutProxy() {
        Properties properties = new Properties();
        Configuration configuration = new Configuration();

        HttpLookupConfig lookupConfig = HttpLookupConfig.builder()
                .url("https://google.com")
                .readableConfig(configuration)
                .properties(properties)
                .httpPostRequestCallback(new Slf4JHttpLookupPostRequestCallback())
                .build();

        HttpClient client = JavaNetHttpClientFactory.createClient(lookupConfig);
        assert(client.authenticator().isEmpty());
        assert(client.proxy().isEmpty());
    }

}
