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

import org.apache.flink.connector.http.sink.HttpSinkRequestEntry;

import lombok.extern.slf4j.Slf4j;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpClient.Version;
import java.net.http.HttpRequest.BodyPublishers;
import java.net.http.HttpRequest.Builder;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;

/** This implementation creates HTTP requests for every processed event. */
@Slf4j
public class PerRequestSubmitter extends AbstractRequestSubmitter {

    public PerRequestSubmitter(
            Properties properties, String[] headersAndValues, HttpClient httpClient) {

        super(properties, headersAndValues, httpClient);
    }

    @Override
    public List<CompletableFuture<JavaNetHttpResponseWrapper>> submit(
            String endpointUrl, List<HttpSinkRequestEntry> requestToSubmit) {

        var endpointUri = URI.create(endpointUrl);
        var responseFutures = new ArrayList<CompletableFuture<JavaNetHttpResponseWrapper>>();

        for (var entry : requestToSubmit) {
            HttpRequest httpRequest = buildHttpRequest(entry, endpointUri);
            var response =
                    httpClient
                            .sendAsync(
                                    httpRequest.getHttpRequest(),
                                    HttpResponse.BodyHandlers.ofString())
                            .exceptionally(
                                    ex -> {
                                        // TODO This will be executed on a ForkJoinPool Thread...
                                        // refactor this someday.
                                        log.error(
                                                "Request fatally failed because of an exception",
                                                ex);
                                        return null;
                                    })
                            .thenApplyAsync(
                                    res -> new JavaNetHttpResponseWrapper(httpRequest, res),
                                    publishingThreadPool);
            responseFutures.add(response);
        }
        return responseFutures;
    }

    private HttpRequest buildHttpRequest(HttpSinkRequestEntry requestEntry, URI endpointUri) {
        Builder requestBuilder =
                java.net.http.HttpRequest.newBuilder()
                        .uri(endpointUri)
                        .version(Version.HTTP_1_1)
                        .timeout(Duration.ofSeconds(httpRequestTimeOutSeconds))
                        .method(
                                requestEntry.method,
                                BodyPublishers.ofByteArray(requestEntry.element));

        if (headersAndValues.length != 0) {
            requestBuilder.headers(headersAndValues);
        }

        return new HttpRequest(
                requestBuilder.build(), List.of(requestEntry.element), requestEntry.method);
    }
}
