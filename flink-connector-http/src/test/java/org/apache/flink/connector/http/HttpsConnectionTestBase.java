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

package org.apache.flink.connector.http;

import org.apache.flink.connector.http.preprocessor.HeaderPreprocessor;
import org.apache.flink.connector.http.sink.httpclient.HttpRequest;
import org.apache.flink.connector.http.table.sink.Slf4jHttpPostRequestCallback;
import org.apache.flink.connector.http.utils.HttpHeaderUtils;

import com.github.tomakehurst.wiremock.WireMockServer;

import java.util.Properties;

/** Https Connection Test Base. */
public abstract class HttpsConnectionTestBase {

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
        this.headerPreprocessor = HttpHeaderUtils.createBasicAuthorizationHeaderPreprocessor();
    }

    public void tearDown() {
        if (wireMockServer != null) {
            wireMockServer.stop();
        }
    }
}
