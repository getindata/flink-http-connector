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

package org.apache.flink.connector.http.clients;

import org.apache.flink.connector.http.sink.HttpSinkInternal;
import org.apache.flink.connector.http.sink.HttpSinkRequestEntry;
import org.apache.flink.connector.http.sink.HttpSinkWriter;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * An HTTP client that is used by {@link HttpSinkWriter} to send HTTP requests processed by {@link
 * HttpSinkInternal}.
 */
public interface SinkHttpClient {

    /**
     * Sends HTTP requests to an external web service.
     *
     * @param requestEntries a set of request entries that should be sent to the destination
     * @param endpointUrl the URL of the endpoint
     * @return the new {@link CompletableFuture} wrapping {@link SinkHttpClientResponse} that
     *     completes when all requests have been sent and returned their statuses
     */
    CompletableFuture<SinkHttpClientResponse> putRequests(
            List<HttpSinkRequestEntry> requestEntries, String endpointUrl);
}
