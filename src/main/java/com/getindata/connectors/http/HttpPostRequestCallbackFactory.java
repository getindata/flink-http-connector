package com.getindata.connectors.http;

import org.apache.flink.table.factories.Factory;

import com.getindata.connectors.http.internal.table.lookup.HttpLookupTableSource;
import com.getindata.connectors.http.internal.table.sink.HttpDynamicSink;

/**
 * The {@link Factory} that dynamically creates and injects {@link HttpPostRequestCallback} to
 * {@link HttpDynamicSink} and {@link HttpLookupTableSource}.
 *
 * <p>Custom implementations of {@link HttpPostRequestCallbackFactory} can be registered along
 * other factories in
 * <pre>resources/META-INF/services/org.apache.flink.table.factories.Factory</pre>
 * file and then referenced by their identifiers in:
 * <ul>
 *   <li>
 *       The HttpSink DDL property field <i>gid.connector.http.sink.request-callback</i>
 *       for HTTP sink.
 *   </li>
 *   <li>
 *       The Http lookup DDL property field <i>gid.connector.http.source.lookup.request-callback</i>
 *       for HTTP lookup.
 *   </li>
 * </ul>
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
 *   'gid.connector.http.sink.request-callback' = 'my-callback'
 * )
 * }</pre>
 *
 * <p>The following example shows the minimum Table API example to create a
 * {@link HttpLookupTableSource} that uses a custom callback created by a factory that
 * returns <i>my-callback</i> as its identifier.
 *
 * <pre>{@code
 * CREATE TABLE httplookup (
 *   id bigint
 * ) with (
 *   'connector' = 'rest-lookup',
 *   'url' = 'http://example.com/myendpoint',
 *   'format' = 'json',
 *   'gid.connector.http.source.lookup.request-callback' = 'my-callback'
 * )
 * }</pre>
 *
 * @param <RequestT> type of the HTTP request wrapper
 */

public interface HttpPostRequestCallbackFactory<RequestT> extends Factory {
    /**
     * @return {@link HttpPostRequestCallback} custom request callback instance
     */
    HttpPostRequestCallback<RequestT> createHttpPostRequestCallback();
}
