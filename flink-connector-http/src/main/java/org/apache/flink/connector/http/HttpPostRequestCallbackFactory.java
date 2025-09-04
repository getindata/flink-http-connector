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

import org.apache.flink.connector.http.table.lookup.HttpLookupTableSource;
import org.apache.flink.connector.http.table.sink.HttpDynamicSink;
import org.apache.flink.table.factories.Factory;

/**
 * The {@link Factory} that dynamically creates and injects {@link HttpPostRequestCallback} to
 * {@link HttpDynamicSink} and {@link HttpLookupTableSource}.
 *
 * <p>Custom implementations of {@link HttpPostRequestCallbackFactory} can be registered along other
 * factories in
 *
 * <pre>resources/META-INF/services/org.apache.flink.table.factories.Factory</pre>
 *
 * <p>file and then referenced by their identifiers in:
 *
 * <ul>
 *   <li>The HttpSink DDL property field <i>http.sink.request-callback</i> for HTTP sink.
 *   <li>The Http lookup DDL property field <i>http.source.lookup.request-callback</i> for HTTP
 *       lookup.
 * </ul>
 *
 * <br>
 *
 * <p>The following example shows the minimum Table API example to create a {@link HttpDynamicSink}
 * that uses a custom callback created by a factory that returns <i>my-callback</i> as its
 * identifier.
 *
 * <pre>{@code
 * CREATE TABLE http (
 *   id bigint,
 *   some_field string
 * ) with (
 *   'connector' = 'http-sink'
 *   'url' = 'http://example.com/myendpoint'
 *   'format' = 'json',
 *   'http.sink.request-callback' = 'my-callback'
 * )
 * }</pre>
 *
 * <p>The following example shows the minimum Table API example to create a {@link
 * HttpLookupTableSource} that uses a custom callback created by a factory that returns
 * <i>my-callback</i> as its identifier.
 *
 * <pre>{@code
 * CREATE TABLE httplookup (
 *   id bigint
 * ) with (
 *   'connector' = 'rest-lookup',
 *   'url' = 'http://example.com/myendpoint',
 *   'format' = 'json',
 *   'http.source.lookup.request-callback' = 'my-callback'
 * )
 * }</pre>
 *
 * @param <RequestT> type of the HTTP request wrapper
 */
public interface HttpPostRequestCallbackFactory<RequestT> extends Factory {
    /** @return {@link HttpPostRequestCallback} custom request callback instance */
    HttpPostRequestCallback<RequestT> createHttpPostRequestCallback();
}
