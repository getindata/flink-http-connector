package com.getindata.connectors.http.internal;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import com.getindata.connectors.http.internal.sink.HttpSinkInternal;
import com.getindata.connectors.http.internal.sink.HttpSinkRequestEntry;
import com.getindata.connectors.http.internal.sink.HttpSinkWriter;

/**
 * An HTTP client that is used by {@link HttpSinkWriter} to send HTTP requests processed by {@link
 * HttpSinkInternal}.
 */
public interface SinkHttpClient {

    /**
     * Sends HTTP requests to an external web service.
     *
     * @param requestEntries a set of request entries that should be sent to the destination
     * @param endpointUrl    the URL of the endpoint
     * @return the new {@link CompletableFuture} wrapping {@link SinkHttpClientResponse} that
     * completes when all requests have been sent and returned their statuses
     */
    CompletableFuture<SinkHttpClientResponse> putRequests(List<HttpSinkRequestEntry> requestEntries,
        String endpointUrl);
}
