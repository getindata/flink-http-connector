package com.getindata.connectors.http.internal.utils;

import java.io.File;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import com.github.tomakehurst.wiremock.WireMockServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.any;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.options;
import static org.assertj.core.api.Assertions.assertThat;

import com.getindata.connectors.http.internal.SinkHttpClientResponse;
import com.getindata.connectors.http.internal.config.HttpConnectorConfigConstants;
import com.getindata.connectors.http.internal.sink.HttpSinkRequestEntry;
import com.getindata.connectors.http.internal.sink.httpclient.JavaNetSinkHttpClient;

// TODO Refactor this to be common for Sink and Lookup Source
class JavaNetSinkHttpClientConnectionTest {

    private static final int SERVER_PORT = 9090;

    private static final int HTTPS_SERVER_PORT = 8443;

    private static final String ENDPOINT = "/myendpoint";

    private static final String CERTS_PATH = "src/test/resources/security/certs/";

    private static final String SERVER_KEYSTORE_PATH =
        "src/test/resources/security/certs/serverKeyStore.jks";

    private static final String SERVER_TRUSTSTORE_PATH =
        "src/test/resources/security/certs/serverTrustStore.jks";

    private WireMockServer wireMockServer;

    private Properties properties;

    @BeforeEach
    public void setUp() {
        this.properties = new Properties();
    }

    @AfterEach
    public void tearDown() {
        wireMockServer.stop();
    }

    @Test
    public void testHttpConnection() throws ExecutionException, InterruptedException {

        wireMockServer = new WireMockServer(SERVER_PORT);
        wireMockServer.start();
        mockEndPoint(wireMockServer);

        JavaNetSinkHttpClient client = new JavaNetSinkHttpClient(new Properties());
        HttpSinkRequestEntry requestEntry = new HttpSinkRequestEntry("GET", new byte[0]);
        SinkHttpClientResponse response =
            client.putRequests(
                Collections.singletonList(requestEntry),
                "http://localhost:" + SERVER_PORT + ENDPOINT
            ).get();

        assertThat(response.getSuccessfulRequests()).isNotEmpty();
        assertThat(response.getFailedRequests()).isEmpty();
    }

    @Test
    public void testHttpsConnectionWithSelfSignedCert() throws Exception {

        File keyStoreFile = new File(SERVER_KEYSTORE_PATH);

        wireMockServer = new WireMockServer(options()
            .httpsPort(HTTPS_SERVER_PORT)
            .httpDisabled(true)
            .keystorePath(keyStoreFile.getAbsolutePath())
            .keystorePassword("password")
            .keyManagerPassword("password")
        );

        wireMockServer.start();
        mockEndPoint(wireMockServer);

        properties.setProperty(HttpConnectorConfigConstants.ALLOW_SELF_SIGNED, "true");

        JavaNetSinkHttpClient client = new JavaNetSinkHttpClient(properties);
        HttpSinkRequestEntry requestEntry = new HttpSinkRequestEntry("GET", new byte[0]);
        SinkHttpClientResponse response =
            client.putRequests(
                Collections.singletonList(requestEntry),
                "https://localhost:" + HTTPS_SERVER_PORT + ENDPOINT
            ).get();

        assertThat(response.getSuccessfulRequests()).isNotEmpty();
        assertThat(response.getFailedRequests()).isEmpty();
    }

    @ParameterizedTest
    @ValueSource(strings = {"ca.crt", "server.crt"})
    public void testHttpsConnectionWithAddedCerts(String certName) throws Exception {

        File keyStoreFile = new File(SERVER_KEYSTORE_PATH);
        File trustedCert = new File(CERTS_PATH + certName);

        wireMockServer = new WireMockServer(options()
            .httpsPort(HTTPS_SERVER_PORT)
            .httpDisabled(true)
            .keystorePath(keyStoreFile.getAbsolutePath())
            .keystorePassword("password")
            .keyManagerPassword("password")
        );
        wireMockServer.start();
        mockEndPoint(wireMockServer);

        properties.setProperty(
            HttpConnectorConfigConstants.SERVER_TRUSTED_CERT,
            trustedCert.getAbsolutePath()
        );

        JavaNetSinkHttpClient client = new JavaNetSinkHttpClient(properties);
        HttpSinkRequestEntry requestEntry = new HttpSinkRequestEntry("GET", new byte[0]);
        SinkHttpClientResponse response =
            client.putRequests(
                Collections.singletonList(requestEntry),
                "https://localhost:" + HTTPS_SERVER_PORT + ENDPOINT
            ).get();

        assertThat(response.getSuccessfulRequests()).isNotEmpty();
        assertThat(response.getFailedRequests()).isEmpty();
    }

    @ParameterizedTest
    @ValueSource(strings = {"clientPrivateKey.pem", "clientPrivateKey.der"})
    public void testMTlsConnection(String clientPrivateKeyName) throws Exception {

        File keyStoreFile = new File(SERVER_KEYSTORE_PATH);
        File trustStoreFile = new File(SERVER_TRUSTSTORE_PATH);
        File serverTrustedCert = new File(CERTS_PATH + "ca.crt");

        File clientCert = new File(CERTS_PATH + "client.crt");
        File clientPrivateKey = new File(CERTS_PATH + clientPrivateKeyName);

        this.wireMockServer = new WireMockServer(options()
            .httpDisabled(true)
            .httpsPort(HTTPS_SERVER_PORT)
            .keystorePath(keyStoreFile.getAbsolutePath())
            .keystorePassword("password")
            .keyManagerPassword("password")
            .needClientAuth(true)
            .trustStorePath(trustStoreFile.getAbsolutePath())
            .trustStorePassword("password")
        );

        wireMockServer.start();

        mockEndPoint(wireMockServer);

        properties.setProperty(
            HttpConnectorConfigConstants.SERVER_TRUSTED_CERT,
            serverTrustedCert.getAbsolutePath()
        );
        properties.setProperty(
            HttpConnectorConfigConstants.CLIENT_CERT,
            clientCert.getAbsolutePath()
        );
        properties.setProperty(
            HttpConnectorConfigConstants.CLIENT_PRIVATE_KEY,
            clientPrivateKey.getAbsolutePath()
        );

        JavaNetSinkHttpClient client = new JavaNetSinkHttpClient(properties);
        HttpSinkRequestEntry requestEntry = new HttpSinkRequestEntry("GET", new byte[0]);
        SinkHttpClientResponse response =
            client.putRequests(
                Collections.singletonList(requestEntry),
                "https://localhost:" + HTTPS_SERVER_PORT + ENDPOINT
            ).get();

        assertThat(response.getSuccessfulRequests()).isNotEmpty();
        assertThat(response.getFailedRequests()).isEmpty();
    }

    private void mockEndPoint(WireMockServer wireMockServer) {
        wireMockServer.stubFor(any(urlPathEqualTo(JavaNetSinkHttpClientConnectionTest.ENDPOINT))
            .willReturn(
                aResponse()
                    .withStatus(200)
                    .withBody("{}"))
        );
    }
}
