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

import java.io.Serializable;
import java.net.http.HttpResponse;
import java.util.Map;

/**
 * An interface for post request callback action, processing a response and its respective request.
 *
 * <p>One can customize the behaviour of such a callback by implementing both {@link
 * HttpPostRequestCallback} and {@link HttpPostRequestCallbackFactory}.
 *
 * @param <RequestT> type of the HTTP request wrapper
 */
public interface HttpPostRequestCallback<RequestT> extends Serializable {
    /**
     * Process HTTP request and the matching response.
     *
     * @param response HTTP response
     * @param requestEntry request's payload
     * @param endpointUrl the URL of the endpoint
     * @param headerMap mapping of header names to header values
     */
    void call(
            HttpResponse<String> response,
            RequestT requestEntry,
            String endpointUrl,
            Map<String, String> headerMap);
}
