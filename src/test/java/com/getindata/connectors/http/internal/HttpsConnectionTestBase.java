package com.getindata.connectors.http.internal;

import java.util.Properties;

import com.github.tomakehurst.wiremock.WireMockServer;

import com.getindata.connectors.http.HttpPostRequestCallback;
import com.getindata.connectors.http.internal.sink.httpclient.HttpRequest;
import com.getindata.connectors.http.internal.table.sink.Slf4jHttpPostRequestCallback;
import com.getindata.connectors.http.internal.utils.HttpHeaderUtils;

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

    protected HeaderPreprocessor headerPreprocessor;

    protected HttpPostRequestCallback<HttpRequest> postRequestCallback =
        new Slf4jHttpPostRequestCallback();

    public void setUp() {
        this.properties = new Properties();
        this.headerPreprocessor = HttpHeaderUtils.createDefaultHeaderPreprocessor();
    }

    public void tearDown() {
        if (wireMockServer != null) {
            wireMockServer.stop();
        }
    }
}
