package com.getindata.connectors.http.internal.utils;

import java.io.File;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import com.github.tomakehurst.wiremock.WireMockServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.any;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.options;
import static org.assertj.core.api.Assertions.assertThat;

import com.getindata.connectors.http.internal.SinkHttpClientResponse;
import com.getindata.connectors.http.internal.config.HttpConnectorConfigConstants;
import com.getindata.connectors.http.internal.sink.HttpSinkRequestEntry;
import com.getindata.connectors.http.internal.sink.httpclient.JavaNetSinkHttpClient;

class JavaNetSinkHttpClientConnectionTest {

    private static final int SERVER_PORT = 9090;

    private static final int HTTPS_SERVER_PORT = 8443;

    private static final String ENDPOINT = "/myendpoint";

    private WireMockServer wireMockServer;

    @AfterEach
    public void tearDown() {
        wireMockServer.stop();
    }

    @Test
    public void testHttpConnection() throws ExecutionException, InterruptedException {

        wireMockServer = new WireMockServer(SERVER_PORT, HTTPS_SERVER_PORT);
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

        File keyStoreFile = new File("src/test/resources/keystore/myKeystore.jks");
        File serverCert = new File("src/test/resources/myCerts/cert.pem");

        wireMockServer = new WireMockServer(options()
            .port(SERVER_PORT)
            .httpsPort(HTTPS_SERVER_PORT)
            .keystorePath(keyStoreFile.getAbsolutePath())
            .keystorePassword("password")
            .keyManagerPassword("password")
        );
        wireMockServer.start();
        mockEndPoint(wireMockServer);

        Properties properties = new Properties();
        //properties.setProperty(HttpConnectorConfigConstants.SELF_SIGNED_CERT, "true");
        properties.setProperty(HttpConnectorConfigConstants.CA_CERT, serverCert.getAbsolutePath());

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

    @Test
    public void foo() throws ExecutionException, InterruptedException {

        this.wireMockServer = new WireMockServer(options()
            .port(SERVER_PORT)
            .httpsPort(HTTPS_SERVER_PORT)
            .needClientAuth(true)
            // Either a path to a file or a resource on the classpath
            .trustStorePath("/path/to/truststore.jks")
            .trustStorePassword("password")
        );

        wireMockServer.start();

        mockEndPoint(wireMockServer);

        Properties properties = new Properties();
        properties.setProperty(HttpConnectorConfigConstants.SELF_SIGNED_CERT, "true");

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
