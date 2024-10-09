
/*
 * Copyright 2020 Red Hat
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.getindata.connectors.http.internal.auth;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

/**
 * This class is inspired by
 * https://github.com/Apicurio/apicurio-common-rest-client/blob/
 * 944ac9eb527c291a6083bd10ee012388e1684d20/rest-client-common/src/main/java/io/
 * apicurio/rest/client/auth/OidcAuth.java.
 *
 * The OIDC access token manager encapsulates the caching of an OIDC access token,
 * which can be short lived, for example an hour. The authenticate method will return an
 * un-expired access token, either from the cache or by requesting a new access token.
 */
@Slf4j
public class OidcAccessTokenManager {

    private static final Duration DEFAULT_TOKEN_EXPIRATION_REDUCTION = Duration.ofSeconds(1);
    private final HttpClient httpClient;
    private final String tokenRequest;

    private final String url;
    private final Duration tokenExpirationReduction;

    private String cachedAccessToken;
    private Instant cachedAccessTokenExp;

    /**
     * Construct an Oidc access token manager with the default token expiration reduction
     * @param httpClient httpClient to use to call the token endpoint.
     * @param tokenRequest token request
     * @param url token endpoint url
     */
    public OidcAccessTokenManager(HttpClient httpClient, String tokenRequest, String url) {
        this(httpClient, tokenRequest, url, DEFAULT_TOKEN_EXPIRATION_REDUCTION);
    }
    /**
     * Construct an Oidc access token manager with the supplied token expiration reduction
     * @param httpClient httpClient to use to call the token endpoint.
     * @param tokenRequest token request this need to be form urlencoded
     * @param url token endpoint url
     * @param tokenExpirationReduction token expiry reduction, request a new token if the
     *                                 current time is later than the cached access token
     *                                 expiry time reduced by this value. This means that
     *                                 we will not use the cached token if it is about
     *                                 to expire.
     */
    public OidcAccessTokenManager(HttpClient httpClient, String tokenRequest, String url,
                                  Duration tokenExpirationReduction)  {
        this.tokenRequest = tokenRequest;
        this.httpClient = httpClient;
        this.url = url;
        if (null == tokenExpirationReduction) {
            this.tokenExpirationReduction = DEFAULT_TOKEN_EXPIRATION_REDUCTION;
        } else {
            this.tokenExpirationReduction = tokenExpirationReduction;
        }
    }

    /**
     * Request an access token from the token endpoint
     */
    private void requestAccessToken() {
        try {
            HttpRequest httpRequest =
                    HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .header("Content-Type", "application/x-www-form-urlencoded")
                    .method("POST", HttpRequest.BodyPublishers.ofString(tokenRequest))
                    .build();

            HttpResponse<byte[]> response = httpClient.send(httpRequest,
                    HttpResponse.BodyHandlers.ofByteArray());
            //create ObjectMapper instance
            final ObjectMapper objectMapper = new ObjectMapper();
            if (200 == response.statusCode()) {
                byte[] bytes = response.body();
                JsonNode rootNode = objectMapper.readTree(bytes);
                JsonNode tokenNode = rootNode.path("access_token");
                JsonNode expiresInNode = rootNode.path("expires_in");
                this.cachedAccessToken = tokenNode.textValue();
                /*
                 expiresIn is in seconds
                */
                Duration expiresIn = Duration.ofSeconds(expiresInNode.asInt());
                if (expiresIn.compareTo(this.tokenExpirationReduction) > 0) {
                    //expiresIn is greater than tokenExpirationReduction
                    expiresIn = expiresIn.minus(this.tokenExpirationReduction);
                }
                this.cachedAccessTokenExp = Instant.now().plus(expiresIn);
            } else {
                throw new IllegalStateException("Attempted to get an access token but got http" +
                        " status code " + response.statusCode());
            }
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Error found while trying to request a new token");
        } catch (IOException e) {
            throw new IllegalStateException("IO Exception occurred",  e);
        } catch (InterruptedException e) {
            throw new IllegalStateException("Interrupted Exception occurred", e);
        }
    }

    /**
     * Get a valid unexpired access token.
     * @return access token.
     */
    public String authenticate() {
        if (isAccessTokenRequired()) {
            requestAccessToken();
        }
        return cachedAccessToken;
    }

    private boolean isAccessTokenRequired() {
        return null == cachedAccessToken || isTokenExpired();
    }

    private boolean isTokenExpired() {
        return Instant.now().isAfter(this.cachedAccessTokenExp);
    }
}
