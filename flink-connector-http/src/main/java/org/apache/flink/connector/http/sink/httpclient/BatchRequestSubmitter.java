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

package org.apache.flink.connector.http.sink.httpclient;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.connector.http.config.HttpConnectorConfigConstants;
import org.apache.flink.connector.http.sink.HttpSinkRequestEntry;

import lombok.extern.slf4j.Slf4j;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpClient.Version;
import java.net.http.HttpRequest.BodyPublisher;
import java.net.http.HttpRequest.BodyPublishers;
import java.net.http.HttpRequest.Builder;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;

/**
 * This implementation groups received events in batches and submits each batch as individual HTTP
 * requests. Batch is created based on batch size or based on HTTP method type.
 */
@Slf4j
public class BatchRequestSubmitter extends AbstractRequestSubmitter {

    private static final byte[] BATCH_START_BYTES = "[".getBytes(StandardCharsets.UTF_8);

    private static final byte[] BATCH_END_BYTES = "]".getBytes(StandardCharsets.UTF_8);

    private static final byte[] BATCH_ELEMENT_DELIM_BYTES = ",".getBytes(StandardCharsets.UTF_8);

    private final int httpRequestBatchSize;

    public BatchRequestSubmitter(
            Properties properties, String[] headersAndValue, HttpClient httpClient) {

        super(properties, headersAndValue, httpClient);

        this.httpRequestBatchSize =
                Integer.parseInt(
                        properties.getProperty(
                                HttpConnectorConfigConstants.SINK_HTTP_BATCH_REQUEST_SIZE));
    }

    @Override
    public List<CompletableFuture<JavaNetHttpResponseWrapper>> submit(
            String endpointUrl, List<HttpSinkRequestEntry> requestsToSubmit) {

        if (requestsToSubmit.isEmpty()) {
            return Collections.emptyList();
        }

        var responseFutures = new ArrayList<CompletableFuture<JavaNetHttpResponseWrapper>>();
        String previousReqeustMethod = requestsToSubmit.get(0).method;
        List<HttpSinkRequestEntry> requestBatch = new ArrayList<>(httpRequestBatchSize);

        for (var entry : requestsToSubmit) {
            if (requestBatch.size() == httpRequestBatchSize
                    || !previousReqeustMethod.equalsIgnoreCase(entry.method)) {
                // break batch and submit
                responseFutures.add(sendBatch(endpointUrl, requestBatch));
                requestBatch.clear();
            }
            requestBatch.add(entry);
            previousReqeustMethod = entry.method;
        }

        // submit anything that left
        responseFutures.add(sendBatch(endpointUrl, requestBatch));
        return responseFutures;
    }

    @VisibleForTesting
    int getBatchSize() {
        return httpRequestBatchSize;
    }

    private CompletableFuture<JavaNetHttpResponseWrapper> sendBatch(
            String endpointUrl, List<HttpSinkRequestEntry> reqeustBatch) {

        HttpRequest httpRequest = buildHttpRequest(reqeustBatch, URI.create(endpointUrl));
        return httpClient
                .sendAsync(httpRequest.getHttpRequest(), HttpResponse.BodyHandlers.ofString())
                .exceptionally(
                        ex -> {
                            // TODO This will be executed on a ForkJoinPool Thread... refactor this
                            // someday.
                            log.error("Request fatally failed because of an exception", ex);
                            return null;
                        })
                .thenApplyAsync(
                        res -> new JavaNetHttpResponseWrapper(httpRequest, res),
                        publishingThreadPool);
    }

    private HttpRequest buildHttpRequest(List<HttpSinkRequestEntry> reqeustBatch, URI endpointUri) {

        try {
            var method = reqeustBatch.get(0).method;
            List<byte[]> elements = new ArrayList<>(reqeustBatch.size());

            BodyPublisher publisher;
            // By default, Java's BodyPublishers.ofByteArrays(elements) will just put Jsons
            // into the HTTP body without any context.
            // What we do here is we pack every Json/byteArray into Json Array hence '[' and ']'
            // at the end, and we separate every element with comma.
            elements.add(BATCH_START_BYTES);
            for (HttpSinkRequestEntry entry : reqeustBatch) {
                elements.add(entry.element);
                elements.add(BATCH_ELEMENT_DELIM_BYTES);
            }
            elements.set(elements.size() - 1, BATCH_END_BYTES);
            publisher = BodyPublishers.ofByteArrays(elements);

            Builder requestBuilder =
                    java.net.http.HttpRequest.newBuilder()
                            .uri(endpointUri)
                            .version(Version.HTTP_1_1)
                            .timeout(Duration.ofSeconds(httpRequestTimeOutSeconds))
                            .method(method, publisher);

            if (headersAndValues.length != 0) {
                requestBuilder.headers(headersAndValues);
            }

            return new HttpRequest(requestBuilder.build(), elements, method);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
