package com.getindata.connectors.http;

import com.getindata.connectors.http.sink.HttpSinkRequestEntry;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * An HTTP client that is used by {@link com.getindata.connectors.http.sink.HttpSinkWriter}
 * to send HTTP requests processed by {@link com.getindata.connectors.http.sink.HttpSink}.
 */
public interface SinkHttpClient {
  /**
   * Returns a {@link CompletableFuture} that completes when all requests have been sent and returned their statuses.
   *
   * @param requestEntries a set of request entries that should be sent to the destination
   * @param endpointUrl    the URL of the endpoint
   * @return the new {@link CompletableFuture} wrapping {@link SinkHttpClientResponse}
   * to be processed by {@code HttpSinkWriter}
   */
  CompletableFuture<SinkHttpClientResponse> putRequests(List<HttpSinkRequestEntry> requestEntries, String endpointUrl);
}
