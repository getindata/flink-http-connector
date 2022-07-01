package com.getindata.connectors.http.sink.httpclient;

import com.getindata.connectors.http.sink.HttpSink;
import com.getindata.connectors.http.sink.HttpSinkRequestEntry;
import lombok.Data;
import lombok.NonNull;

import java.net.http.HttpResponse;
import java.util.Optional;

/**
 * A wrapper structure around an HTTP response, keeping a reference to a particular
 * {@link HttpSinkRequestEntry}. Used internally by the {@code HttpSinkWriter} to pass
 * {@code HttpSinkRequestEntry} along some other element that it is logically connected with.
 */
@Data
final class JavaNetHttpResponseWrapper {
  /**
   * A representation of a single {@link HttpSink} request.
   */
  @NonNull
  private final HttpSinkRequestEntry sinkRequestEntry;

  /**
   * A response to an HTTP request based on {@link HttpSinkRequestEntry}.
   */
  private final HttpResponse<String> response;

  public Optional<HttpResponse<String>> getResponse() {
    return Optional.ofNullable(response);
  }
}
