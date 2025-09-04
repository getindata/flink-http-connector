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

package org.apache.flink.connector.http.auth;

import net.minidev.json.JSONObject;
import org.junit.jupiter.api.Test;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSession;

import java.net.Authenticator;
import java.net.CookieHandler;
import java.net.ProxySelector;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpHeaders;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Test for {@link OidcAccessTokenManager}. */
public class OidcAccessTokenManagerTest {

    private static final String BASE_URL = "http://localhost/aaa";

    @Test
    public void testAuthenticate() throws InterruptedException {

        MockHttpClient authHttpClient = new MockHttpClient();

        authHttpClient.setIsExpired(1);
        authHttpClient.setAccessToken("Access1");
        String url = BASE_URL;
        OidcAccessTokenManager oidcAuth = new OidcAccessTokenManager(authHttpClient, "abc", url);

        // apply the authorization to the httpRequest
        String token1 = oidcAuth.authenticate();
        assertThat(token1).isNotNull();
        String token2 = oidcAuth.authenticate();
        assertThat(token2).isNotNull();
        // check the token is cached
        assertThat(token1).isEqualTo(token2);
        Thread.sleep(2000);
        // check the token is different after first token has expired
        String token3 = oidcAuth.authenticate();
        assertThat(token3).isNotNull();
        assertThat(token3).isNotEqualTo(token2);
    }

    @Test
    public void testAuthenticateWithBadStatusCode() throws InterruptedException {

        MockHttpClient authHttpClient = new MockHttpClient();

        authHttpClient.setIsExpired(1);
        authHttpClient.setAccessToken("Access1");
        authHttpClient.setStatus(500);
        String url = BASE_URL;
        OidcAccessTokenManager oidcAuth = new OidcAccessTokenManager(authHttpClient, "abc", url);

        try {
            oidcAuth.authenticate();
            assertTrue(false, "Bad status code should result in an exception.");
        } catch (IllegalStateException e) {
            // expected
        }
    }

    @Test
    public void testAuthenticateWithExpiryReduction() throws InterruptedException {

        MockHttpClient authHttpClient = new MockHttpClient();

        authHttpClient.setIsExpired(1);
        authHttpClient.setAccessToken("Access1");
        String url = "http://localhost";
        OidcAccessTokenManager oidcAuth =
                new OidcAccessTokenManager(authHttpClient, "abc", url, Duration.ofSeconds(5));

        // apply the authorization to the httpRequest
        String token1 = oidcAuth.authenticate();
        assertThat(token1).isNotNull();
        String token2 = oidcAuth.authenticate();
        assertThat(token2).isNotNull();
    }

    class MockHttpClient extends HttpClient {
        private int isExpired;
        private String accessToken;
        private int count = 0;
        private int status = 200;

        @Override
        public Optional<CookieHandler> cookieHandler() {
            return Optional.empty();
        }

        @Override
        public Optional<Duration> connectTimeout() {
            return Optional.empty();
        }

        @Override
        public Redirect followRedirects() {
            return null;
        }

        @Override
        public Optional<ProxySelector> proxy() {
            return Optional.empty();
        }

        @Override
        public SSLContext sslContext() {
            return null;
        }

        @Override
        public SSLParameters sslParameters() {
            return null;
        }

        @Override
        public Optional<Authenticator> authenticator() {
            return Optional.empty();
        }

        @Override
        public Version version() {
            return null;
        }

        @Override
        public Optional<Executor> executor() {
            return Optional.empty();
        }

        @Override
        public <T> HttpResponse<T> send(
                HttpRequest request, HttpResponse.BodyHandler<T> responseBodyHandler) {

            JSONObject json = new JSONObject();

            json.put("expires_in", 2);
            json.put("access_token", "dummy_token_" + this.count++);
            byte[] bytes = json.toJSONString().getBytes();

            MockHttpResponse mockHttpResponse = new MockHttpResponse();
            mockHttpResponse.setStatusCode(status);
            mockHttpResponse.setBody(bytes);

            return (HttpResponse<T>) mockHttpResponse;
        }

        @Override
        public <T> CompletableFuture<HttpResponse<T>> sendAsync(
                HttpRequest request, HttpResponse.BodyHandler<T> responseBodyHandler) {
            return null;
        }

        @Override
        public <T> CompletableFuture<HttpResponse<T>> sendAsync(
                HttpRequest request,
                HttpResponse.BodyHandler<T> responseBodyHandler,
                HttpResponse.PushPromiseHandler<T> pushPromiseHandler) {
            return null;
        }

        public void setIsExpired(int isExpired) {
            this.isExpired = isExpired;
        }

        public void setAccessToken(String accesstoken) {
            this.accessToken = accesstoken;
        }

        public void setStatus(int status) {
            this.status = status;
        }

        class MockHttpResponse implements HttpResponse<byte[]> {
            int statusCode = 0;
            byte[] body = new byte[0];

            @Override
            public int statusCode() {
                return statusCode;
            }

            @Override
            public HttpRequest request() {
                return null;
            }

            @Override
            public Optional<HttpResponse<byte[]>> previousResponse() {
                return Optional.empty();
            }

            @Override
            public HttpHeaders headers() {
                return null;
            }

            @Override
            public byte[] body() {
                return body;
            }

            @Override
            public Optional<SSLSession> sslSession() {
                return Optional.empty();
            }

            @Override
            public URI uri() {
                return null;
            }

            @Override
            public Version version() {
                return null;
            }

            public void setStatusCode(int statusCode) {
                this.statusCode = statusCode;
            }

            public void setBody(byte[] body) {
                this.body = body;
            }
        }
    }
}
