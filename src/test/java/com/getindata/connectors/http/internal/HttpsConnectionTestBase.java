package com.getindata.connectors.http.internal;

import java.util.Properties;

import com.github.tomakehurst.wiremock.WireMockServer;

public abstract class HttpsConnectionTestBase {

    protected static final int SERVER_PORT = 9090;

    protected static final int HTTPS_SERVER_PORT = 8443;

    protected static final String ENDPOINT = "/myendpoint";

    protected static final String CERTS_PATH = "src/test/resources/security/certs/";

    protected static final String SERVER_KEYSTORE_PATH =
        "src/test/resources/security/certs/serverKeyStore.jks";

    protected static final String SERVER_TRUSTSTORE_PATH =
        "src/test/resources/security/certs/serverTrustStore.jks";

    protected WireMockServer wireMockServer;

    protected Properties properties;

    public void setUp() {
        this.properties = new Properties();
    }

    public void tearDown() {
        if (wireMockServer != null) {
            wireMockServer.stop();
        }
    }
}
