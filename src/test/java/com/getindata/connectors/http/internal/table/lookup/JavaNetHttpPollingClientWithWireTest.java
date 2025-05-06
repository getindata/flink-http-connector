package com.getindata.connectors.http.internal.table.lookup;

import java.io.File;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.time.Duration;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.ConfigurationException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import com.getindata.connectors.http.internal.HeaderPreprocessor;
import com.getindata.connectors.http.internal.utils.HttpHeaderUtils;
import static com.getindata.connectors.http.TestHelper.readTestFile;
import static com.getindata.connectors.http.internal.table.lookup.HttpLookupConnectorOptions.SOURCE_LOOKUP_OIDC_AUTH_TOKEN_ENDPOINT_URL;
import static com.getindata.connectors.http.internal.table.lookup.HttpLookupConnectorOptions.SOURCE_LOOKUP_OIDC_AUTH_TOKEN_EXPIRY_REDUCTION;
import static com.getindata.connectors.http.internal.table.lookup.HttpLookupConnectorOptions.SOURCE_LOOKUP_OIDC_AUTH_TOKEN_REQUEST;

public class JavaNetHttpPollingClientWithWireTest {
    private static final String BASE_URL = "http://localhost.com";

    private static final String SAMPLES_FOLDER = "/auth/";
    private static final int SERVER_PORT = 9090;

    private static final int HTTPS_SERVER_PORT = 8443;

    private static final String SERVER_KEYSTORE_PATH =
            "src/test/resources/security/certs/serverKeyStore.jks";

    private static final String SERVER_TRUSTSTORE_PATH =
            "src/test/resources/security/certs/serverTrustStore.jks";

    private static final String ENDPOINT = "/auth";
    private static final String BEARER_REQUEST = "Bearer Dummy";

    private WireMockServer wireMockServer;

    @SuppressWarnings("unchecked")
    @BeforeEach
    public void setup() {

        File keyStoreFile = new File(SERVER_KEYSTORE_PATH);
        File trustStoreFile = new File(SERVER_TRUSTSTORE_PATH);

        wireMockServer = new WireMockServer(
                WireMockConfiguration.wireMockConfig()
                        .port(SERVER_PORT)
                        .httpsPort(HTTPS_SERVER_PORT)
                        .keystorePath(keyStoreFile.getAbsolutePath())
                        .keystorePassword("password")
                        .keyManagerPassword("password")
                        .needClientAuth(true)
                        .trustStorePath(trustStoreFile.getAbsolutePath())
                        .trustStorePassword("password")
                        .extensions(JsonTransform.class)
        );
        wireMockServer.start();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRestartStrategy(RestartStrategies.noRestart());
        Configuration config = new Configuration();
        config.set(ExecutionOptions.RUNTIME_MODE, RuntimeExecutionMode.STREAMING);
        env.configure(config, getClass().getClassLoader());
        env.enableCheckpointing(1000, CheckpointingMode.EXACTLY_ONCE);
    }

    @AfterEach
    public void tearDown() {
        wireMockServer.stop();
    }


    @Test
    public void shouldUpdateHttpRequestIfRequiredGet() throws ConfigurationException {
        HttpRequest httpRequest = HttpRequest.newBuilder()
                .GET()
                .uri(URI.create(BASE_URL))
                .timeout(Duration.ofSeconds(1))
                .setHeader("Origin", "*")
                .setHeader("X-Content-Type-Options", "nosniff")
                .setHeader("Content-Type", "application/json")
                .build();
        shouldUpdateHttpRequestIfRequired(httpRequest);
    }

    @Test
    public void shouldUpdateHttpRequestIfRequiredPut() throws ConfigurationException {
        HttpRequest httpRequest = HttpRequest.newBuilder()
                .PUT(HttpRequest.BodyPublishers.ofString("foo"))
                .uri(URI.create(BASE_URL))
                .timeout(Duration.ofSeconds(1))
                .setHeader("Origin", "*")
                .setHeader("X-Content-Type-Options", "nosniff")
                .setHeader("Content-Type", "application/json")
                .build();
        shouldUpdateHttpRequestIfRequired(httpRequest);
    }

    private void shouldUpdateHttpRequestIfRequired(HttpRequest httpRequest) throws ConfigurationException {
        setUpServerBodyStub();
        JavaNetHttpPollingClient client = new JavaNetHttpPollingClient(mock(HttpClient.class),
                null,
                HttpLookupConfig.builder().url(BASE_URL).build(),
                null);
        LookupQueryInfo lookupQueryInfo = null;
        HttpLookupSourceRequestEntry request =
                new HttpLookupSourceRequestEntry(httpRequest, lookupQueryInfo);

        Configuration configuration = new Configuration();
        HeaderPreprocessor oidcHeaderPreProcessor =
                HttpHeaderUtils.createOIDCHeaderPreprocessor(configuration);
        HttpRequest newHttpRequest = client.updateHttpRequestIfRequired(request,
                oidcHeaderPreProcessor);
        assertThat(httpRequest).isEqualTo(newHttpRequest);
        configuration.setString(SOURCE_LOOKUP_OIDC_AUTH_TOKEN_ENDPOINT_URL.key(), "http://localhost:9090/auth");
        configuration.setString(SOURCE_LOOKUP_OIDC_AUTH_TOKEN_REQUEST, BEARER_REQUEST);
        configuration.set(SOURCE_LOOKUP_OIDC_AUTH_TOKEN_EXPIRY_REDUCTION,
                Duration.ofSeconds(1L));
        client = new JavaNetHttpPollingClient(mock(HttpClient.class),
                null,
                HttpLookupConfig.builder().url(BASE_URL).readableConfig(configuration).build(),
                null);
        oidcHeaderPreProcessor =
                HttpHeaderUtils.createOIDCHeaderPreprocessor(configuration);
        // change oidcHeaderPreProcessor to use the mock http client for the authentication flow
        newHttpRequest = client.updateHttpRequestIfRequired(request,
                oidcHeaderPreProcessor);
        assertThat(httpRequest).isNotEqualTo(newHttpRequest);
        assertThat(httpRequest.headers().map().keySet().size()).isEqualTo(3);
        assertThat(newHttpRequest.headers().map().keySet().size()).isEqualTo(4);
        assertThat(httpRequest.headers().map().get("Content-Type"))
                .isEqualTo(newHttpRequest.headers().map().get("Content-Type"));
    }

    private void setUpServerBodyStub() {
        wireMockServer.stubFor(
                post(urlEqualTo(ENDPOINT))
                        .withHeader("Content-Type", equalTo("application/x-www-form-urlencoded"))
                        .withRequestBody(equalTo(BEARER_REQUEST))
                        .willReturn(
                                aResponse()
                                        .withStatus(200)
                                        .withBody(readTestFile(SAMPLES_FOLDER + "AuthResult.json"))
                        )
        );
    }
}
