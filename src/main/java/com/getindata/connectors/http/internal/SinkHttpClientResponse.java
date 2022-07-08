package com.getindata.connectors.http.internal;

import com.getindata.connectors.http.internal.sink.HttpSinkRequestEntry;
import lombok.Data;
import lombok.NonNull;

import java.util.List;

/**
 * Data class holding {@link HttpSinkRequestEntry} instances that {@link SinkHttpClient} attempted
 * to write, divided into two lists &mdash; successful and failed ones.
 */
@Data
public class SinkHttpClientResponse {
  /**
   * A list of successfully written requests.
   */
  @NonNull
  private final List<HttpSinkRequestEntry> successfulRequests;

  /**
   * A list of requests that {@link SinkHttpClient} failed to write.
   */
  @NonNull
  private final List<HttpSinkRequestEntry> failedRequests;
}
