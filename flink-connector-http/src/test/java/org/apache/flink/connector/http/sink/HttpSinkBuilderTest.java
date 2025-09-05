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

package org.apache.flink.connector.http.sink;

import org.apache.flink.connector.base.sink.writer.ElementConverter;
import org.apache.flink.connector.http.HttpSink;
import org.apache.flink.connector.http.clients.SinkHttpClient;
import org.apache.flink.connector.http.clients.SinkHttpClientResponse;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertThrows;

/** Test for {@link HttpSink }. */
public class HttpSinkBuilderTest {

    private static final ElementConverter<String, HttpSinkRequestEntry> ELEMENT_CONVERTER =
            (s, context) -> new HttpSinkRequestEntry("POST", s.getBytes(StandardCharsets.UTF_8));

    @Test
    public void testEmptyUrl() {
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        HttpSink.<String>builder()
                                .setElementConverter(ELEMENT_CONVERTER)
                                .setSinkHttpClientBuilder(
                                        (properties,
                                                httpPostRequestCallback,
                                                headerPreprocessor,
                                                requestSubmitterFactory) -> new MockHttpClient())
                                .setEndpointUrl("")
                                .build());
    }

    @Test
    public void testNullUrl() {
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        HttpSink.<String>builder()
                                .setElementConverter(ELEMENT_CONVERTER)
                                .setSinkHttpClientBuilder(
                                        (properties,
                                                httpPostRequestCallback,
                                                headerPreprocessor,
                                                requestSubmitterFactory) -> new MockHttpClient())
                                .build());
    }

    @Test
    public void testNullHttpClient() {
        assertThrows(
                NullPointerException.class,
                () ->
                        HttpSink.<String>builder()
                                .setElementConverter(ELEMENT_CONVERTER)
                                .setSinkHttpClientBuilder(null)
                                .setEndpointUrl("localhost:8000")
                                .build());
    }

    private static class MockHttpClient implements SinkHttpClient {

        MockHttpClient() {}

        @Override
        public CompletableFuture<SinkHttpClientResponse> putRequests(
                List<HttpSinkRequestEntry> requestEntries, String endpointUrl) {
            throw new RuntimeException("Mock implementation of HttpClient");
        }
    }
}
