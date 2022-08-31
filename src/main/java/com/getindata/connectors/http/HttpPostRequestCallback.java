package com.getindata.connectors.http;

import java.io.Serializable;
import java.net.http.HttpResponse;
import java.util.Map;

import com.getindata.connectors.http.internal.sink.HttpSinkRequestEntry;

/**
 * An interface for post request callback action, processing a response and its respective request.
 *
 * <p>One can customize the behaviour of such a callback by implementing both
 * {@link HttpPostRequestCallback} and {@link HttpPostRequestCallbackFactory}.
 */
public interface HttpPostRequestCallback extends Serializable {
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
