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

import org.apache.flink.connector.http.HttpPostRequestCallback;
import org.apache.flink.connector.http.config.HttpConnectorConfigConstants;
import org.apache.flink.connector.http.preprocessor.HeaderPreprocessor;
import org.apache.flink.connector.http.table.sink.Slf4jHttpPostRequestCallback;
import org.apache.flink.connector.http.utils.HttpHeaderUtils;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;

import java.net.http.HttpClient;
import java.util.Properties;
import java.util.stream.Stream;

import static org.apache.flink.connector.http.TestHelper.assertPropertyArray;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

/** Test for {@link JavaNetSinkHttpClient }. */
@ExtendWith(MockitoExtension.class)
class JavaNetSinkHttpClientTest {

    private static MockedStatic<HttpClient> httpClientStaticMock;

    @Mock private HttpClient.Builder httpClientBuilder;

    @BeforeAll
    public static void beforeAll() {
        httpClientStaticMock = mockStatic(HttpClient.class);
    }

    protected HeaderPreprocessor headerPreprocessor;

    protected HttpPostRequestCallback<HttpRequest> postRequestCallback;

    @AfterAll
    public static void afterAll() {
        if (httpClientStaticMock != null) {
            httpClientStaticMock.close();
        }
    }

    @BeforeEach
    public void setUp() {
        postRequestCallback = new Slf4jHttpPostRequestCallback();
        headerPreprocessor = HttpHeaderUtils.createBasicAuthorizationHeaderPreprocessor();
        httpClientStaticMock.when(HttpClient::newBuilder).thenReturn(httpClientBuilder);
        when(httpClientBuilder.followRedirects(any())).thenReturn(httpClientBuilder);
        when(httpClientBuilder.sslContext(any())).thenReturn(httpClientBuilder);
        when(httpClientBuilder.executor(any())).thenReturn(httpClientBuilder);
    }

    private static Stream<Arguments> provideSubmitterFactory() {
        return Stream.of(
                Arguments.of(new PerRequestRequestSubmitterFactory()),
                Arguments.of(new BatchRequestSubmitterFactory(50)));
    }

    @ParameterizedTest
    @MethodSource("provideSubmitterFactory")
    public void shouldBuildClientWithoutHeaders(RequestSubmitterFactory requestSubmitterFactory) {

        JavaNetSinkHttpClient client =
                new JavaNetSinkHttpClient(
                        new Properties(),
                        postRequestCallback,
                        this.headerPreprocessor,
                        requestSubmitterFactory);
        assertThat(client.getHeadersAndValues()).isEmpty();
    }

    @ParameterizedTest
    @MethodSource("provideSubmitterFactory")
    public void shouldBuildClientWithHeaders(RequestSubmitterFactory requestSubmitterFactory) {

        // GIVEN
        Properties properties = new Properties();
        properties.setProperty("property", "val1");
        properties.setProperty("my.property", "val2");
        properties.setProperty(
                HttpConnectorConfigConstants.SINK_HEADER_PREFIX + "Origin",
                "https://developer.mozilla.org");
        properties.setProperty(
                HttpConnectorConfigConstants.SINK_HEADER_PREFIX + "Cache-Control",
                "no-cache, no-store, max-age=0, must-revalidate");
        properties.setProperty(
                HttpConnectorConfigConstants.SINK_HEADER_PREFIX + "Access-Control-Allow-Origin",
                "*");

        // WHEN
        JavaNetSinkHttpClient client =
                new JavaNetSinkHttpClient(
                        properties,
                        postRequestCallback,
                        headerPreprocessor,
                        requestSubmitterFactory);
        String[] headersAndValues = client.getHeadersAndValues();
        assertThat(headersAndValues).hasSize(6);

        // THEN
        // assert that we have property followed by its value.
        assertPropertyArray(headersAndValues, "Origin", "https://developer.mozilla.org");
        assertPropertyArray(
                headersAndValues,
                "Cache-Control",
                "no-cache, no-store, max-age=0, must-revalidate");
        assertPropertyArray(headersAndValues, "Access-Control-Allow-Origin", "*");
    }
}
