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

package org.apache.flink.connector.http.sink.httpclient;

import org.apache.flink.connector.http.HttpsConnectionTestBase;
import org.apache.flink.connector.http.WireMockServerPortAllocator;
import org.apache.flink.connector.http.clients.SinkHttpClientResponse;
import org.apache.flink.connector.http.config.HttpConnectorConfigConstants;
import org.apache.flink.connector.http.sink.HttpSinkRequestEntry;

import com.github.tomakehurst.wiremock.WireMockServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.util.Collections;
import java.util.Properties;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.any;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.options;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Test for {@link JavaNetSinkHttpClient }. */
class JavaNetSinkHttpClientConnectionTest extends HttpsConnectionTestBase {

    private RequestSubmitterFactory perRequestSubmitterFactory;

    private RequestSubmitterFactory batchRequestSubmitterFactory;

    public static int serverPort;

    public static int httpsServerPort;

    @BeforeEach
    public void setUp() {
        super.setUp();
        serverPort = WireMockServerPortAllocator.getServerPort();
        httpsServerPort = WireMockServerPortAllocator.getSecureServerPort();

        this.perRequestSubmitterFactory = new PerRequestRequestSubmitterFactory();
        this.batchRequestSubmitterFactory = new BatchRequestSubmitterFactory(50);
    }

    @AfterEach
    public void tearDown() {
        super.tearDown();
    }

    @Test
    public void testHttpConnection() {

        wireMockServer = new WireMockServer(serverPort);
        wireMockServer.start();
        mockEndPoint(wireMockServer);

        testSinkClientForConnection(
                new Properties(), "http://localhost:", serverPort, perRequestSubmitterFactory);

        testSinkClientForConnection(
                new Properties(), "http://localhost:", serverPort, batchRequestSubmitterFactory);
    }

    @Test
    public void testHttpsConnectionWithSelfSignedCert() {

        File keyStoreFile = new File(SERVER_KEYSTORE_PATH);

        wireMockServer =
                new WireMockServer(
                        options()
                                .httpsPort(httpsServerPort)
                                .httpDisabled(true)
                                .keystorePath(keyStoreFile.getAbsolutePath())
                                .keystorePassword("password")
                                .keyManagerPassword("password"));

        wireMockServer.start();
        mockEndPoint(wireMockServer);

        properties.setProperty(HttpConnectorConfigConstants.ALLOW_SELF_SIGNED, "true");

        testSinkClientForConnection(
                properties, "https://localhost:", httpsServerPort, perRequestSubmitterFactory);

        testSinkClientForConnection(
                properties, "https://localhost:", httpsServerPort, batchRequestSubmitterFactory);
    }

    @ParameterizedTest
    @ValueSource(strings = {"ca.crt", "server.crt", "ca_server_bundle.cert.pem"})
    public void testHttpsConnectionWithAddedCerts(String certName) {

        File keyStoreFile = new File(SERVER_KEYSTORE_PATH);
        File trustedCert = new File(CERTS_PATH + certName);

        wireMockServer =
                new WireMockServer(
                        options()
                                .httpsPort(httpsServerPort)
                                .httpDisabled(true)
                                .keystorePath(keyStoreFile.getAbsolutePath())
                                .keystorePassword("password")
                                .keyManagerPassword("password"));

        wireMockServer.start();
        mockEndPoint(wireMockServer);

        properties.setProperty(
                HttpConnectorConfigConstants.SERVER_TRUSTED_CERT, trustedCert.getAbsolutePath());

        testSinkClientForConnection(
                properties, "https://localhost:", httpsServerPort, perRequestSubmitterFactory);

        testSinkClientForConnection(
                properties, "https://localhost:", httpsServerPort, batchRequestSubmitterFactory);
    }

    @ParameterizedTest
    @ValueSource(strings = {"clientPrivateKey.pem", "clientPrivateKey.der"})
    public void testMTlsConnection(String clientPrivateKeyName) {

        File keyStoreFile = new File(SERVER_KEYSTORE_PATH);
        File trustStoreFile = new File(SERVER_TRUSTSTORE_PATH);
        File serverTrustedCert = new File(CERTS_PATH + "ca.crt");

        File clientCert = new File(CERTS_PATH + "client.crt");
        File clientPrivateKey = new File(CERTS_PATH + clientPrivateKeyName);

        this.wireMockServer =
                new WireMockServer(
                        options()
                                .httpDisabled(true)
                                .httpsPort(httpsServerPort)
                                .keystorePath(keyStoreFile.getAbsolutePath())
                                .keystorePassword("password")
                                .keyManagerPassword("password")
                                .needClientAuth(true)
                                .trustStorePath(trustStoreFile.getAbsolutePath())
                                .trustStorePassword("password"));

        wireMockServer.start();
        mockEndPoint(wireMockServer);

        properties.setProperty(
                HttpConnectorConfigConstants.SERVER_TRUSTED_CERT,
                serverTrustedCert.getAbsolutePath());
        properties.setProperty(
                HttpConnectorConfigConstants.CLIENT_CERT, clientCert.getAbsolutePath());
        properties.setProperty(
                HttpConnectorConfigConstants.CLIENT_PRIVATE_KEY,
                clientPrivateKey.getAbsolutePath());

        testSinkClientForConnection(
                properties, "https://localhost:", httpsServerPort, perRequestSubmitterFactory);

        testSinkClientForConnection(
                properties, "https://localhost:", httpsServerPort, batchRequestSubmitterFactory);
    }

    @Test
    public void testMTlsConnectionUsingKeyStore() {
        String password = "password";

        String clientKeyStoreName = "client_keyStore.p12";
        String serverKeyStoreName = "serverKeyStore.jks";
        String serverTrustStoreName = "serverTrustStore.jks";

        File clientKeyStoreFile = new File(CERTS_PATH + clientKeyStoreName);
        File serverKeyStoreFile = new File(CERTS_PATH + serverKeyStoreName);
        File serverTrustStoreFile = new File(CERTS_PATH + serverTrustStoreName);
        File serverTrustedCert = new File(CERTS_PATH + "ca_server_bundle.cert.pem");

        this.wireMockServer =
                new WireMockServer(
                        options()
                                .httpDisabled(true)
                                .httpsPort(httpsServerPort)
                                .keystorePath(serverKeyStoreFile.getAbsolutePath())
                                .keystorePassword(password)
                                .keyManagerPassword(password)
                                .needClientAuth(true)
                                .trustStorePath(serverTrustStoreFile.getAbsolutePath())
                                .trustStorePassword(password));

        wireMockServer.start();
        mockEndPoint(wireMockServer);

        properties.setProperty(HttpConnectorConfigConstants.KEY_STORE_PASSWORD, password);
        properties.setProperty(
                HttpConnectorConfigConstants.KEY_STORE_PATH, clientKeyStoreFile.getAbsolutePath());
        properties.setProperty(
                HttpConnectorConfigConstants.SERVER_TRUSTED_CERT,
                serverTrustedCert.getAbsolutePath());

        testSinkClientForConnection(
                properties, "https://localhost:", httpsServerPort, perRequestSubmitterFactory);

        testSinkClientForConnection(
                properties, "https://localhost:", httpsServerPort, batchRequestSubmitterFactory);
    }

    @ParameterizedTest
    @CsvSource(
            value = {
                "invalid.crt, client.crt, clientPrivateKey.pem",
                "ca.crt, invalid.crt, clientPrivateKey.pem",
                "ca.crt, client.crt, invalid.pem"
            })
    public void shouldThrowOnInvalidPath(
            String serverCertName, String clientCertName, String clientKeyName) {

        File serverTrustedCert = new File(CERTS_PATH + serverCertName);
        File clientCert = new File(CERTS_PATH + clientCertName);
        File clientPrivateKey = new File(CERTS_PATH + clientKeyName);

        properties.setProperty(
                HttpConnectorConfigConstants.SERVER_TRUSTED_CERT,
                serverTrustedCert.getAbsolutePath());
        properties.setProperty(
                HttpConnectorConfigConstants.CLIENT_CERT, clientCert.getAbsolutePath());
        properties.setProperty(
                HttpConnectorConfigConstants.CLIENT_PRIVATE_KEY,
                clientPrivateKey.getAbsolutePath());

        assertAll(
                () -> {
                    assertThrows(
                            RuntimeException.class,
                            () ->
                                    new JavaNetSinkHttpClient(
                                            properties,
                                            postRequestCallback,
                                            headerPreprocessor,
                                            perRequestSubmitterFactory));
                    assertThrows(
                            RuntimeException.class,
                            () ->
                                    new JavaNetSinkHttpClient(
                                            properties,
                                            postRequestCallback,
                                            headerPreprocessor,
                                            batchRequestSubmitterFactory));
                });
    }

    @ParameterizedTest
    @ValueSource(strings = {"user:password", "Basic dXNlcjpwYXNzd29yZA=="})
    public void shouldConnectWithBasicAuth(String authorizationHeaderValue) {

        wireMockServer = new WireMockServer(serverPort);
        wireMockServer.start();
        mockEndPointWithBasicAuth(wireMockServer);

        properties.setProperty(
                HttpConnectorConfigConstants.SINK_HEADER_PREFIX + "Authorization",
                authorizationHeaderValue);

        testSinkClientForConnection(
                properties, "http://localhost:", serverPort, perRequestSubmitterFactory);

        testSinkClientForConnection(
                properties, "http://localhost:", serverPort, batchRequestSubmitterFactory);
    }

    private void testSinkClientForConnection(
            Properties properties,
            String endpointUrl,
            int httpsServerPort,
            RequestSubmitterFactory requestSubmitterFactory) {

        try {
            JavaNetSinkHttpClient client =
                    new JavaNetSinkHttpClient(
                            properties,
                            postRequestCallback,
                            headerPreprocessor,
                            requestSubmitterFactory);
            HttpSinkRequestEntry requestEntry = new HttpSinkRequestEntry("GET", new byte[0]);
            SinkHttpClientResponse response =
                    client.putRequests(
                                    Collections.singletonList(requestEntry),
                                    endpointUrl + httpsServerPort + ENDPOINT)
                            .get();

            assertThat(response.getSuccessfulRequests()).isNotEmpty();
            assertThat(response.getFailedRequests()).isEmpty();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void mockEndPoint(WireMockServer wireMockServer) {
        wireMockServer.stubFor(
                any(urlPathEqualTo(ENDPOINT))
                        .willReturn(aResponse().withStatus(200).withBody("{}")));
    }

    private void mockEndPointWithBasicAuth(WireMockServer wireMockServer) {

        wireMockServer.stubFor(
                any(urlPathEqualTo(ENDPOINT))
                        .withBasicAuth("user", "password")
                        .willReturn(aResponse().withStatus(200).withBody("{}")));
    }
}
