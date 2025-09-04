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

package org.apache.flink.connector.http;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.connector.base.sink.AsyncSinkBaseBuilder;
import org.apache.flink.connector.base.sink.writer.ElementConverter;
import org.apache.flink.connector.http.clients.SinkHttpClient;
import org.apache.flink.connector.http.clients.SinkHttpClientBuilder;
import org.apache.flink.connector.http.preprocessor.HeaderPreprocessor;
import org.apache.flink.connector.http.sink.HttpSinkRequestEntry;
import org.apache.flink.connector.http.sink.httpclient.HttpRequest;
import org.apache.flink.connector.http.sink.httpclient.JavaNetSinkHttpClient;
import org.apache.flink.connector.http.table.sink.Slf4jHttpPostRequestCallback;
import org.apache.flink.connector.http.utils.HttpHeaderUtils;

import java.util.Optional;
import java.util.Properties;

/**
 * Builder to construct {@link HttpSink}.
 *
 * <p>The following example shows the minimum setup to create a {@link HttpSink} that writes String
 * values to an HTTP endpoint using POST method.
 *
 * <pre>{@code
 * HttpSink<String> httpSink =
 *     HttpSink.<String>builder()
 *             .setEndpointUrl("http://example.com/myendpoint")
 *             .setElementConverter(
 *                 (s, _context) -> new HttpSinkRequestEntry(
 *                 "POST",
 *                 s.getBytes(StandardCharsets.UTF_8)))
 *             .build();
 * }</pre>
 *
 * <p>If the following parameters are not set in this builder, the following defaults will be used:
 *
 * <ul>
 *   <li>{@code maxBatchSize} will be 500,
 *   <li>{@code maxInFlightRequests} will be 50,
 *   <li>{@code maxBufferedRequests} will be 10000,
 *   <li>{@code maxBatchSizeInBytes} will be 5 MB i.e. {@code 5 * 1024 * 1024},
 *   <li>{@code maxTimeInBufferMS} will be 5000ms,
 *   <li>{@code maxRecordSizeInBytes} will be 1 MB i.e. {@code 1024 * 1024}.
 * </ul>
 *
 * {@code endpointUrl} and {@code elementConverter} must be set by the user.
 *
 * @param <InputT> type of the elements that should be sent through HTTP request.
 */
public class HttpSinkBuilder<InputT>
        extends AsyncSinkBaseBuilder<InputT, HttpSinkRequestEntry, HttpSinkBuilder<InputT>> {

    private static final int DEFAULT_MAX_BATCH_SIZE = 500;

    private static final int DEFAULT_MAX_IN_FLIGHT_REQUESTS = 50;

    private static final int DEFAULT_MAX_BUFFERED_REQUESTS = 10_000;

    private static final long DEFAULT_MAX_BATCH_SIZE_IN_B = 5 * 1024 * 1024;

    private static final long DEFAULT_MAX_TIME_IN_BUFFER_MS = 5000;

    private static final long DEFAULT_MAX_RECORD_SIZE_IN_B = 1024 * 1024;

    private static final SinkHttpClientBuilder DEFAULT_CLIENT_BUILDER = JavaNetSinkHttpClient::new;

    private static final HttpPostRequestCallback<HttpRequest> DEFAULT_POST_REQUEST_CALLBACK =
            new Slf4jHttpPostRequestCallback();

    private static final HeaderPreprocessor DEFAULT_HEADER_PREPROCESSOR =
            HttpHeaderUtils.createBasicAuthorizationHeaderPreprocessor();

    private final Properties properties = new Properties();

    // Mandatory field
    private String endpointUrl;

    // Mandatory field
    private ElementConverter<InputT, HttpSinkRequestEntry> elementConverter;

    // If not defined, should be set to DEFAULT_CLIENT_BUILDER
    private SinkHttpClientBuilder sinkHttpClientBuilder;

    // If not defined, should be set to DEFAULT_POST_REQUEST_CALLBACK
    private HttpPostRequestCallback<HttpRequest> httpPostRequestCallback;

    // If not defined, should be set to DEFAULT_HEADER_PREPROCESSOR
    private HeaderPreprocessor headerPreprocessor;

    HttpSinkBuilder() {
        this.sinkHttpClientBuilder = DEFAULT_CLIENT_BUILDER;
        this.httpPostRequestCallback = DEFAULT_POST_REQUEST_CALLBACK;
        this.headerPreprocessor = DEFAULT_HEADER_PREPROCESSOR;
    }

    /**
     * @param endpointUrl the URL of the endpoint
     * @return {@link HttpSinkBuilder} itself
     */
    public HttpSinkBuilder<InputT> setEndpointUrl(String endpointUrl) {
        this.endpointUrl = endpointUrl;
        return this;
    }

    /**
     * @param sinkHttpClientBuilder builder for an implementation of {@link SinkHttpClient} that
     *     will be used by {@link HttpSink}
     * @return {@link HttpSinkBuilder} itself
     */
    public HttpSinkBuilder<InputT> setSinkHttpClientBuilder(
            SinkHttpClientBuilder sinkHttpClientBuilder) {
        this.sinkHttpClientBuilder = sinkHttpClientBuilder;
        return this;
    }

    /**
     * @param elementConverter the {@link ElementConverter} to be used for the sink
     * @return {@link HttpSinkBuilder} itself
     * @deprecated Converters set by this method might not work properly for Flink 1.16+. Use {@link
     *     #setElementConverter(SchemaLifecycleAwareElementConverter)} instead.
     */
    @Deprecated
    @PublicEvolving
    public HttpSinkBuilder<InputT> setElementConverter(
            ElementConverter<InputT, HttpSinkRequestEntry> elementConverter) {
        this.elementConverter = elementConverter;
        return this;
    }

    /**
     * @param elementConverter the {@link SchemaLifecycleAwareElementConverter} to be used for the
     *     sink
     * @return {@link HttpSinkBuilder} itself
     */
    @PublicEvolving
    public HttpSinkBuilder<InputT> setElementConverter(
            SchemaLifecycleAwareElementConverter<InputT, HttpSinkRequestEntry> elementConverter) {
        this.elementConverter = elementConverter;
        return this;
    }

    public HttpSinkBuilder<InputT> setHttpPostRequestCallback(
            HttpPostRequestCallback<HttpRequest> httpPostRequestCallback) {
        this.httpPostRequestCallback = httpPostRequestCallback;
        return this;
    }

    public HttpSinkBuilder<InputT> setHttpHeaderPreprocessor(
            HeaderPreprocessor headerPreprocessor) {
        this.headerPreprocessor = headerPreprocessor;
        return this;
    }

    /**
     * Set property for Http Sink.
     *
     * @param propertyName property name
     * @param propertyValue property value
     * @return {@link HttpSinkBuilder} itself
     */
    public HttpSinkBuilder<InputT> setProperty(String propertyName, String propertyValue) {
        this.properties.setProperty(propertyName, propertyValue);
        return this;
    }

    /**
     * Add properties to Http Sink configuration.
     *
     * @param properties properties to add
     * @return {@link HttpSinkBuilder} itself
     */
    public HttpSinkBuilder<InputT> setProperties(Properties properties) {
        this.properties.putAll(properties);
        return this;
    }

    @Override
    public HttpSink<InputT> build() {
        return new HttpSink<>(
                elementConverter,
                Optional.ofNullable(getMaxBatchSize()).orElse(DEFAULT_MAX_BATCH_SIZE),
                Optional.ofNullable(getMaxInFlightRequests())
                        .orElse(DEFAULT_MAX_IN_FLIGHT_REQUESTS),
                Optional.ofNullable(getMaxBufferedRequests()).orElse(DEFAULT_MAX_BUFFERED_REQUESTS),
                Optional.ofNullable(getMaxBatchSizeInBytes()).orElse(DEFAULT_MAX_BATCH_SIZE_IN_B),
                Optional.ofNullable(getMaxTimeInBufferMS()).orElse(DEFAULT_MAX_TIME_IN_BUFFER_MS),
                Optional.ofNullable(getMaxRecordSizeInBytes()).orElse(DEFAULT_MAX_RECORD_SIZE_IN_B),
                endpointUrl,
                httpPostRequestCallback,
                headerPreprocessor,
                sinkHttpClientBuilder,
                properties);
    }
}
