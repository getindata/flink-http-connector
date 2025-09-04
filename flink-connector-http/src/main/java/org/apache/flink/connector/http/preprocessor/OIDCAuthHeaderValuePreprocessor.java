/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.http.preprocessor;

import org.apache.flink.connector.http.auth.OidcAccessTokenManager;

import lombok.extern.slf4j.Slf4j;

import java.net.http.HttpClient;
import java.time.Duration;
import java.util.Optional;

/** Header processor for HTTP OIDC Authentication mechanism. */
@Slf4j
public class OIDCAuthHeaderValuePreprocessor implements HeaderValuePreprocessor {

    private final String oidcAuthURL;
    private final String oidcTokenRequest;
    private Duration oidcExpiryReduction = Duration.ofSeconds(1);

    /**
     * Add the access token to the request using OidcAuth authenticate method that gives us a valid
     * access token.
     *
     * @param oidcAuthURL OIDC token endpoint
     * @param oidcTokenRequest OIDC Token Request
     * @param oidcExpiryReduction OIDC token expiry reduction
     */
    public OIDCAuthHeaderValuePreprocessor(
            String oidcAuthURL, String oidcTokenRequest, Optional<Duration> oidcExpiryReduction) {
        this.oidcAuthURL = oidcAuthURL;
        this.oidcTokenRequest = oidcTokenRequest;
        if (oidcExpiryReduction.isPresent()) {
            this.oidcExpiryReduction = oidcExpiryReduction.get();
        }
    }

    @Override
    public String preprocessHeaderValue(String rawValue) {
        OidcAccessTokenManager auth =
                new OidcAccessTokenManager(
                        HttpClient.newBuilder().build(),
                        oidcTokenRequest,
                        oidcAuthURL,
                        oidcExpiryReduction);
        // apply the OIDC authentication by adding the dynamically calculated header value.
        return "BEARER " + auth.authenticate();
    }
}
