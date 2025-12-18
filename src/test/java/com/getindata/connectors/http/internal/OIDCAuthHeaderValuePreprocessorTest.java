package com.getindata.connectors.http.internal;

import java.time.Duration;
import java.util.Optional;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static org.assertj.core.api.Assertions.assertThat;

class OIDCAuthHeaderValuePreprocessorTest {

    private static final int SERVER_PORT = 9091;
    private static final String TOKEN_ENDPOINT = "/oauth/token";

    private WireMockServer wireMockServer;

    @BeforeEach
    public void setup() {
        wireMockServer = new WireMockServer(
                WireMockConfiguration.wireMockConfig().port(SERVER_PORT)
        );
        wireMockServer.start();
    }

    @AfterEach
    public void tearDown() {
        wireMockServer.stop();
    }

    @Test
    public void shouldReturnBearerTokenWithCorrectCasing() {
        // Setup mock OIDC token endpoint
        String accessToken = "test_access_token_12345";
        wireMockServer.stubFor(post(urlEqualTo(TOKEN_ENDPOINT))
                .willReturn(aResponse()
                        .withStatus(200)
                        .withHeader("Content-Type", "application/json")
                        .withBody("{\"access_token\": \"" + accessToken + "\", \"expires_in\": 3600}")
                ));

        String tokenEndpointUrl = "http://localhost:" + SERVER_PORT + TOKEN_ENDPOINT;
        String tokenRequest = "grant_type=client_credentials&client_id=test&client_secret=secret";

        OIDCAuthHeaderValuePreprocessor preprocessor = new OIDCAuthHeaderValuePreprocessor(
                tokenEndpointUrl,
                tokenRequest,
                Optional.of(Duration.ofSeconds(1))
        );

        String headerValue = preprocessor.preprocessHeaderValue("ignored");

        // Verify the Bearer token uses correct RFC 6750 casing ("Bearer" not "BEARER")
        assertThat(headerValue).startsWith("Bearer ");
        assertThat(headerValue).isEqualTo("Bearer " + accessToken);
        // Explicitly verify it's NOT using uppercase BEARER
        assertThat(headerValue).doesNotStartWith("BEARER ");
    }

    @Test
    public void shouldReturnBearerTokenWithDefaultExpiryReduction() {
        // Setup mock OIDC token endpoint
        String accessToken = "another_test_token";
        wireMockServer.stubFor(post(urlEqualTo(TOKEN_ENDPOINT))
                .willReturn(aResponse()
                        .withStatus(200)
                        .withHeader("Content-Type", "application/json")
                        .withBody("{\"access_token\": \"" + accessToken + "\", \"expires_in\": 3600}")
                ));

        String tokenEndpointUrl = "http://localhost:" + SERVER_PORT + TOKEN_ENDPOINT;
        String tokenRequest = "grant_type=client_credentials";

        OIDCAuthHeaderValuePreprocessor preprocessor = new OIDCAuthHeaderValuePreprocessor(
                tokenEndpointUrl,
                tokenRequest,
                Optional.empty()
        );

        String headerValue = preprocessor.preprocessHeaderValue("any_raw_value");

        // Verify correct Bearer casing per RFC 6750
        assertThat(headerValue).startsWith("Bearer ");
        assertThat(headerValue).isEqualTo("Bearer " + accessToken);
    }
}
