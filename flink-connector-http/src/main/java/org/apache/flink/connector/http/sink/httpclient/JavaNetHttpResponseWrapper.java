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

import org.apache.flink.connector.http.HttpSink;
import org.apache.flink.connector.http.sink.HttpSinkRequestEntry;

import lombok.Data;
import lombok.NonNull;

import java.net.http.HttpResponse;
import java.util.Optional;

/**
 * A wrapper structure around an HTTP response, keeping a reference to a particular {@link
 * HttpSinkRequestEntry}. Used internally by the {@code HttpSinkWriter} to pass {@code
 * HttpSinkRequestEntry} along some other element that it is logically connected with.
 */
@Data
final class JavaNetHttpResponseWrapper {

    /** A representation of a single {@link HttpSink} request. */
    @NonNull private final HttpRequest httpRequest;

    /** A response to an HTTP request based on {@link HttpSinkRequestEntry}. */
    private final HttpResponse<String> response;

    public Optional<HttpResponse<String>> getResponse() {
        return Optional.ofNullable(response);
    }
}
