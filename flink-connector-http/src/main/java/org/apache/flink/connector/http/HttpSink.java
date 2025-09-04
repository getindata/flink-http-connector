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
import org.apache.flink.connector.base.sink.writer.ElementConverter;
import org.apache.flink.connector.http.clients.SinkHttpClientBuilder;
import org.apache.flink.connector.http.preprocessor.HeaderPreprocessor;
import org.apache.flink.connector.http.sink.HttpSinkInternal;
import org.apache.flink.connector.http.sink.HttpSinkRequestEntry;
import org.apache.flink.connector.http.sink.httpclient.HttpRequest;

import java.util.Properties;

/**
 * A public implementation for {@code HttpSink} that performs async requests against a specified
 * HTTP endpoint using the buffering protocol specified in {@link
 * org.apache.flink.connector.base.sink.AsyncSinkBase}.
 *
 * <p>To create a new instance of this class use {@link HttpSinkBuilder}. An example would be:
 *
 * <pre>{@code
 * HttpSink<String> httpSink =
 *     HttpSink.<String>builder()
 *             .setEndpointUrl("http://example.com/myendpoint")
 *             .setElementConverter(
 *                 (s, _context) -> new HttpSinkRequestEntry("POST", "text/plain",
 *                 s.getBytes(StandardCharsets.UTF_8)))
 *             .build();
 * }</pre>
 *
 * @param <InputT> type of the elements that should be sent through HTTP request.
 */
@PublicEvolving
public class HttpSink<InputT> extends HttpSinkInternal<InputT> {

    HttpSink(
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
                maxRecordSizeInBytes,
                endpointUrl,
                httpPostRequestCallback,
                headerPreprocessor,
                sinkHttpClientBuilder,
                properties);
    }

    /**
     * Create a {@link HttpSinkBuilder} constructing a new {@link HttpSink}.
     *
     * @param <InputT> type of the elements that should be sent through HTTP request
     * @return {@link HttpSinkBuilder}
     */
    public static <InputT> HttpSinkBuilder<InputT> builder() {
        return new HttpSinkBuilder<>();
    }
}
