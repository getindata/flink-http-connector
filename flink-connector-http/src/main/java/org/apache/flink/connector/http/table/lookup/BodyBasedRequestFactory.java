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
import java.net.http.HttpRequest.BodyPublishers;
import java.net.http.HttpRequest.Builder;
import java.time.Duration;

/**
 * Implementation of {@link HttpRequestFactory} for REST calls that sends their parameters using
 * request body or in the path.
 */
@Slf4j
public class BodyBasedRequestFactory extends RequestFactoryBase {

    private final String methodName;

    public BodyBasedRequestFactory(
            String methodName,
            LookupQueryCreator lookupQueryCreator,
            HeaderPreprocessor headerPreprocessor,
            HttpLookupConfig options) {

        super(lookupQueryCreator, headerPreprocessor, options);
        this.methodName = methodName.toUpperCase();
    }

    /**
     * Method for preparing {@link Builder} for REST request that sends their parameters in request
     * body, for example PUT or POST methods.
     *
     * @param lookupQueryInfo lookup query info used for request body.
     * @return {@link Builder} for given lookupQuery.
     */
    @Override
    protected Builder setUpRequestMethod(LookupQueryInfo lookupQueryInfo) {
        return HttpRequest.newBuilder()
                .uri(constructUri(lookupQueryInfo))
                .method(methodName, BodyPublishers.ofString(lookupQueryInfo.getLookupQuery()))
                .timeout(Duration.ofSeconds(this.httpRequestTimeOutSeconds));
    }

    @Override
    protected Logger getLogger() {
        return log;
    }

    URI constructUri(LookupQueryInfo lookupQueryInfo) {
        StringBuilder resolvedUrl = new StringBuilder(baseUrl);
        if (lookupQueryInfo.hasBodyBasedUrlQueryParameters()) {
            resolvedUrl
                    .append(baseUrl.contains("?") ? "&" : "?")
                    .append(lookupQueryInfo.getBodyBasedUrlQueryParameters());
        }
        resolvedUrl = resolvePathParameters(lookupQueryInfo, resolvedUrl);

        try {
            return new URIBuilder(resolvedUrl.toString()).build();
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }
}
