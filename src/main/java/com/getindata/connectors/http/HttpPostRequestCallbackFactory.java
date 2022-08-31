package com.getindata.connectors.http;

import org.apache.flink.table.factories.Factory;

import com.getindata.connectors.http.internal.table.sink.HttpDynamicSink;

/**
 * The {@link Factory} that dynamically creates and injects {@link HttpPostRequestCallback} to
 * {@link HttpDynamicSink}.
 *
 * <p>Custom implementations of {@link HttpPostRequestCallbackFactory} can be registered along
 * other factories in
 * <pre>resources/META-INF.services/org.apache.flink.table.factories.Factory</pre>
 * file and then referenced by their identifiers in the HttpSink DDL property field
 * <i>gid.connector.http.sink.request-callback</i>.
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
 * @param <RequestT> type of the HTTP request wrapper
 */
public interface HttpPostRequestCallbackFactory<RequestT> extends Factory {
    /**
     * @return {@link HttpPostRequestCallback} custom request callback instance
     */
    HttpPostRequestCallback<RequestT> createHttpPostRequestCallback();
}
