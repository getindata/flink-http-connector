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

import org.apache.flink.connector.http.HttpPostRequestCallback;
import org.apache.flink.connector.http.utils.ConfigUtils;

import lombok.extern.slf4j.Slf4j;

import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.StringJoiner;

/**
 * A {@link HttpPostRequestCallback} that logs pairs of request and response as <i>INFO</i> level
 * logs using <i>Slf4j</i>.
 *
 * <p>Serving as a default implementation of {@link HttpPostRequestCallback} for the {@link
 * HttpLookupTableSource}.
 */
@Slf4j
public class Slf4JHttpLookupPostRequestCallback
        implements HttpPostRequestCallback<HttpLookupSourceRequestEntry> {

    @Override
    public void call(
            HttpResponse<String> response,
            HttpLookupSourceRequestEntry requestEntry,
            String endpointUrl,
            Map<String, String> headerMap) {

        HttpRequest httpRequest = requestEntry.getHttpRequest();
        StringJoiner headers = new StringJoiner(";");

        for (Entry<String, List<String>> reqHeaders : httpRequest.headers().map().entrySet()) {
            StringJoiner values = new StringJoiner(";");
            for (String value : reqHeaders.getValue()) {
                values.add(value);
            }
            String header = reqHeaders.getKey() + ": [" + values + "]";
            headers.add(header);
        }

        if (response == null) {
            log.warn("Null Http response for request " + httpRequest.uri().toString());

            log.info(
                    "Got response for a request.\n  Request:\n    URL: {}\n    "
                            + "Method: {}\n    Headers: {}\n    Params/Body: {}\nResponse: null",
                    httpRequest.uri().toString(),
                    httpRequest.method(),
                    headers,
                    requestEntry.getLookupQueryInfo());
        } else {
            log.info(
                    "Got response for a request.\n  Request:\n    URL: {}\n    "
                            + "Method: {}\n    Headers: {}\n    Params/Body: {}\nResponse: {}\n    Body: {}",
                    httpRequest.uri().toString(),
                    httpRequest.method(),
                    headers,
                    requestEntry.getLookupQueryInfo(),
                    response,
                    response.body().replaceAll(ConfigUtils.UNIVERSAL_NEW_LINE_REGEXP, ""));
        }
    }
}
