package com.getindata.connectors.http;

import java.io.Serializable;
import java.net.http.HttpResponse;
import java.util.Map;

import com.getindata.connectors.http.internal.sink.HttpSinkRequestEntry;
import com.getindata.connectors.http.internal.table.sink.HttpDynamicSink;

/**
 * An interface for a custom class that logs responses and requests processed by the HTTP Sink.
 *
 * <p>{@link HttpDynamicSink} processes responses that it gets from the HTTP endpoint along their
 * respective requests. One can customize the behaviour of such a processor by implementing both
 * {@link HttpSinkPostRequestCallback} and {@link HttpSinkPostRequestCallbackFactory}.
 */
public interface HttpSinkPostRequestCallback extends Serializable {
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
        HttpSinkRequestEntry requestEntry,
        String endpointUrl,
        Map<String, String> headerMap
    );
}
