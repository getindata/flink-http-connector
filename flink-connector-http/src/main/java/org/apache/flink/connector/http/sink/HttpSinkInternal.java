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

package org.apache.flink.connector.http.sink;

import org.apache.flink.connector.base.sink.AsyncSinkBase;
import org.apache.flink.connector.base.sink.writer.BufferedRequestState;
import org.apache.flink.connector.base.sink.writer.ElementConverter;
import org.apache.flink.connector.http.HttpPostRequestCallback;
import org.apache.flink.connector.http.HttpSink;
import org.apache.flink.connector.http.HttpSinkBuilder;
import org.apache.flink.connector.http.SchemaLifecycleAwareElementConverter;
import org.apache.flink.connector.http.clients.SinkHttpClientBuilder;
import org.apache.flink.connector.http.config.HttpConnectorConfigConstants;
import org.apache.flink.connector.http.config.SinkRequestSubmitMode;
import org.apache.flink.connector.http.preprocessor.HeaderPreprocessor;
import org.apache.flink.connector.http.sink.httpclient.BatchRequestSubmitterFactory;
import org.apache.flink.connector.http.sink.httpclient.HttpRequest;
import org.apache.flink.connector.http.sink.httpclient.PerRequestRequestSubmitterFactory;
import org.apache.flink.connector.http.sink.httpclient.RequestSubmitterFactory;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StringUtils;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Properties;

/**
 * An internal implementation of HTTP Sink that performs async requests against a specified HTTP
 * endpoint using the buffering protocol specified in {@link AsyncSinkBase}.
 *
 * <p>API of this class can change without any concerns as long as it does not have any influence on
 * methods defined in {@link HttpSink} and {@link HttpSinkBuilder} classes.
 *
 * <p>The behaviour of the buffering may be specified by providing configuration during the sink
 * build time.
 *
 * <ul>
 *   <li>{@code maxBatchSize}: the maximum size of a batch of entries that may be sent to the HTTP
 *       endpoint;
 *   <li>{@code maxInFlightRequests}: the maximum number of in flight requests that may exist, if
 *       any more in flight requests need to be initiated once the maximum has been reached, then it
 *       will be blocked until some have completed;
 *   <li>{@code maxBufferedRequests}: the maximum number of elements held in the buffer, requests to
 *       add elements will be blocked while the number of elements in the buffer is at the maximum;
 *   <li>{@code maxBatchSizeInBytes}: the maximum size of a batch of entries that may be sent to the
 *       HTTP endpoint measured in bytes;
 *   <li>{@code maxTimeInBufferMS}: the maximum amount of time an entry is allowed to live in the
 *       buffer, if any element reaches this age, the entire buffer will be flushed immediately;
 *   <li>{@code maxRecordSizeInBytes}: the maximum size of a record the sink will accept into the
 *       buffer, a record of size larger than this will be rejected when passed to the sink.
 *   <li>{@code httpPostRequestCallback}: the {@link HttpPostRequestCallback} implementation for
 *       processing of requests and responses;
 *   <li>{@code properties}: properties related to the Http Sink.
 * </ul>
 *
 * @param <InputT> type of the elements that should be sent through HTTP request.
 */
public class HttpSinkInternal<InputT> extends AsyncSinkBase<InputT, HttpSinkRequestEntry> {

    private final String endpointUrl;

    // having Builder instead of an instance of `SinkHttpClient`
    // makes it possible to serialize `HttpSink`
    private final SinkHttpClientBuilder sinkHttpClientBuilder;

    private final HttpPostRequestCallback<HttpRequest> httpPostRequestCallback;

    private final HeaderPreprocessor headerPreprocessor;

    private final Properties properties;

    protected HttpSinkInternal(
            ElementConverter<InputT, HttpSinkRequestEntry> elementConverter,
            int maxBatchSize,
            int maxInFlightRequests,
            int maxBufferedRequests,
            long maxBatchSizeInBytes,
            long maxTimeInBufferMS,
            long maxRecordSizeInBytes,
            String endpointUrl,
            HttpPostRequestCallback<HttpRequest> httpPostRequestCallback,
            HeaderPreprocessor headerPreprocessor,
            SinkHttpClientBuilder sinkHttpClientBuilder,
            Properties properties) {

        super(
                elementConverter,
                maxBatchSize,
                maxInFlightRequests,
                maxBufferedRequests,
                maxBatchSizeInBytes,
                maxTimeInBufferMS,
                maxRecordSizeInBytes);

        Preconditions.checkArgument(
                !StringUtils.isNullOrWhitespaceOnly(endpointUrl),
                "The endpoint URL must be set when initializing HTTP Sink.");
        this.endpointUrl = endpointUrl;
        this.httpPostRequestCallback =
                Preconditions.checkNotNull(
                        httpPostRequestCallback,
                        "Post request callback must be set when initializing HTTP Sink.");
        this.headerPreprocessor =
                Preconditions.checkNotNull(
                        headerPreprocessor,
                        "Header Preprocessor must be set when initializing HTTP Sink.");
        this.sinkHttpClientBuilder =
                Preconditions.checkNotNull(
                        sinkHttpClientBuilder,
                        "The HTTP client builder must not be null when initializing HTTP Sink.");
        this.properties = properties;
    }

    @Override
    public StatefulSinkWriter<InputT, BufferedRequestState<HttpSinkRequestEntry>> createWriter(
            InitContext context) throws IOException {

        ElementConverter<InputT, HttpSinkRequestEntry> elementConverter = getElementConverter();
        if (elementConverter instanceof SchemaLifecycleAwareElementConverter) {
            // This cast is needed for Flink 1.15.3 build
            ((SchemaLifecycleAwareElementConverter<?, ?>) elementConverter).open(context);
        }

        return new HttpSinkWriter<>(
                elementConverter,
                context,
                getMaxBatchSize(),
                getMaxInFlightRequests(),
                getMaxBufferedRequests(),
                getMaxBatchSizeInBytes(),
                getMaxTimeInBufferMS(),
                getMaxRecordSizeInBytes(),
                endpointUrl,
                sinkHttpClientBuilder.build(
                        properties,
                        httpPostRequestCallback,
                        headerPreprocessor,
                        getRequestSubmitterFactory()),
                Collections.emptyList(),
                properties);
    }

    @Override
    public StatefulSinkWriter<InputT, BufferedRequestState<HttpSinkRequestEntry>> restoreWriter(
            InitContext context,
            Collection<BufferedRequestState<HttpSinkRequestEntry>> recoveredState)
            throws IOException {

        return new HttpSinkWriter<>(
                getElementConverter(),
                context,
                getMaxBatchSize(),
                getMaxInFlightRequests(),
                getMaxBufferedRequests(),
                getMaxBatchSizeInBytes(),
                getMaxTimeInBufferMS(),
                getMaxRecordSizeInBytes(),
                endpointUrl,
                sinkHttpClientBuilder.build(
                        properties,
                        httpPostRequestCallback,
                        headerPreprocessor,
                        getRequestSubmitterFactory()),
                recoveredState,
                properties);
    }

    @Override
    public SimpleVersionedSerializer<BufferedRequestState<HttpSinkRequestEntry>>
            getWriterStateSerializer() {
        return new HttpSinkWriterStateSerializer();
    }

    private RequestSubmitterFactory getRequestSubmitterFactory() {

        if (SinkRequestSubmitMode.SINGLE
                .getMode()
                .equalsIgnoreCase(
                        properties.getProperty(
                                HttpConnectorConfigConstants.SINK_HTTP_REQUEST_MODE))) {
            return new PerRequestRequestSubmitterFactory();
        }
        return new BatchRequestSubmitterFactory(getMaxBatchSize());
    }
}
