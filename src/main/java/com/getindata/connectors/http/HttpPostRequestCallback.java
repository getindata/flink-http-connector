package com.getindata.connectors.http;

import java.io.Serializable;
import java.net.http.HttpResponse;
import java.util.Map;

/**
 * An interface for post request callback action, processing a response and its respective request.
 *
 * <p>One can customize the behaviour of such a callback by implementing both
 * {@link HttpPostRequestCallback} and {@link HttpPostRequestCallbackFactory}.
 *
 * @param <RequestT> type of the HTTP request wrapper
 */
public interface HttpPostRequestCallback<RequestT> extends Serializable {
    /**
     * Process HTTP request and the matching response.
     *  @param response HTTP response
     * @param requestEntry request's payload
     * @param endpointUrl the URL of the endpoint
     * @param headerMap mapping of header names to header values
     */
    void call(
        HttpResponse<String> response,
        RequestT requestEntry,
        String endpointUrl,
        Map<String, String> headerMap
    );
}
