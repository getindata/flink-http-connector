package com.getindata.connectors.http.internal.utils;

import java.net.Authenticator;
import java.net.InetAddress;
import java.net.PasswordAuthentication;
import java.net.UnknownHostException;
import java.net.http.HttpClient;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;

import com.getindata.connectors.http.internal.table.lookup.HttpLookupConfig;
import com.getindata.connectors.http.internal.table.lookup.Slf4JHttpLookupPostRequestCallback;
import static com.getindata.connectors.http.internal.config.HttpConnectorConfigConstants.*;

class JavaNetHttpClientFactoryTest {

    @Test
    public void shouldGetClientWithAuthenticator() throws UnknownHostException {
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

        assertThat(client.authenticator().isPresent()).isTrue();
        assertThat(client.proxy().isPresent()).isTrue();

        PasswordAuthentication auth = client.authenticator().get().requestPasswordAuthenticationInstance(
                "google",                      // host
                InetAddress.getByName("127.0.0.1"), // address
                8080,                                 // port
                "http",                             // protocol
                "Please authenticate",              // prompt
                "basic",                            // scheme
                null,                               // URL
                Authenticator.RequestorType.PROXY  // Requestor type
        );

        assertThat(auth.getUserName().equals("username")).isTrue();
        assertThat(Arrays.equals(auth.getPassword(), "password".toCharArray())).isTrue();
    }

    @Test
    public void shouldGetClientWithoutAuthenticator() throws UnknownHostException {
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

        assertThat(client.authenticator().isEmpty()).isTrue();
        assertThat(client.proxy().isPresent()).isTrue();
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
                                ThreadUtils.LOGGING_EXCEPTION_HANDLER)
                );

        HttpClient client = JavaNetHttpClientFactory.createClient(properties, httpClientExecutor);
        assertThat(client.followRedirects().equals(HttpClient.Redirect.NORMAL)).isTrue();
    }

}
