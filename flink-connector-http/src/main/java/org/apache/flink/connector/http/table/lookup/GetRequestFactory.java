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

package org.apache.flink.connector.http.table.lookup;

import org.apache.flink.connector.http.LookupQueryCreator;
import org.apache.flink.connector.http.preprocessor.HeaderPreprocessor;
import org.apache.flink.connector.http.utils.uri.URIBuilder;

import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;

import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpRequest;
import java.net.http.HttpRequest.Builder;
import java.time.Duration;

/** Implementation of {@link HttpRequestFactory} for GET REST calls. */
@Slf4j
public class GetRequestFactory extends RequestFactoryBase {

    public GetRequestFactory(
            LookupQueryCreator lookupQueryCreator,
            HeaderPreprocessor headerPreprocessor,
            HttpLookupConfig options) {

        super(lookupQueryCreator, headerPreprocessor, options);
    }

    @Override
    protected Logger getLogger() {
        return log;
    }

    /**
     * Method for preparing {@link Builder} for REST GET request. Where lookupQueryInfo is used as
     * query parameters for GET requests. for example:
     *
     * <pre>
     *     http:localhost:8080/service?id=1
     * </pre>
     *
     * <p>or as payload for body-based requests with optional parameters, for example:
     *
     * <pre>
     *     http:localhost:8080/service?id=1
     *     body payload: { "uid": 2 }
     * </pre>
     *
     * @param lookupQueryInfo lookup query info used for request query parameters.
     * @return {@link Builder} for given GET lookupQuery
     */
    @Override
    protected Builder setUpRequestMethod(LookupQueryInfo lookupQueryInfo) {
        return HttpRequest.newBuilder()
                .uri(constructGetUri(lookupQueryInfo))
                .GET()
                .timeout(Duration.ofSeconds(this.httpRequestTimeOutSeconds));
    }

    URI constructGetUri(LookupQueryInfo lookupQueryInfo) {
        StringBuilder resolvedUrl = new StringBuilder(baseUrl);
        if (lookupQueryInfo.hasLookupQuery()) {
            resolvedUrl
                    .append(baseUrl.contains("?") ? "&" : "?")
                    .append(lookupQueryInfo.getLookupQuery());
        }
        resolvedUrl = resolvePathParameters(lookupQueryInfo, resolvedUrl);
        try {
            return new URIBuilder(resolvedUrl.toString()).build();
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }
}
