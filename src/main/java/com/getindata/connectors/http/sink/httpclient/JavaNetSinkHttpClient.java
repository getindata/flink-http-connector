package com.getindata.connectors.http.sink.httpclient;

import com.getindata.connectors.http.SinkHttpClient;
import com.getindata.connectors.http.SinkHttpClientResponse;
import com.getindata.connectors.http.sink.HttpSink;
import com.getindata.connectors.http.sink.HttpSinkRequestEntry;
import lombok.EqualsAndHashCode;
import lombok.RequiredArgsConstructor;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/**
 * An implementation of {@link SinkHttpClient} that uses Java 11's {@link HttpClient}.
 */
public class JavaNetSinkHttpClient implements SinkHttpClient {
  private final HttpClient httpClient;

  public JavaNetSinkHttpClient() {
    this.httpClient = HttpClient.newBuilder().followRedirects(HttpClient.Redirect.NORMAL).build();
  }

  /**
   * A wrapper structure around an arbitrary element, keeping a reference to a particular
   * {@link HttpSinkRequestEntry}. Used internally by the {@code HttpSinkWriter} to pass
   * {@code HttpSinkRequestEntry} along some other element that is logically connected with it
   * (e.g., full HTTP request built from the {@code HttpSinkRequestEntry}).
   *
   * @param <T>
   */
  @RequiredArgsConstructor
  @EqualsAndHashCode
  private static class HttpSinkRequestEntryWrapper<T> {
    /**
     * An element logically connected with the {@link HttpSinkRequestEntry}.
     */
    public final T element;

    /**
     * A representation of a single {@link HttpSink} request.
     */
    public final HttpSinkRequestEntry sinkRequestEntry;
  }

  @Override
  public CompletableFuture<SinkHttpClientResponse> putRequests(
      List<HttpSinkRequestEntry> requestEntries, String endpointUrl
  ) {
    var endpointUri = URI.create(endpointUrl);

    List<HttpSinkRequestEntryWrapper<HttpRequest>> requests = requestEntries
        .stream()
        .map(requestEntry -> new HttpSinkRequestEntryWrapper<>(
            buildHttpRequest(requestEntry, endpointUri),
            requestEntry
        ))
        .collect(Collectors.toList());

    return getCompletedFutures(requests).thenApply(this::prepareHttpClientResponse);
  }

  private HttpRequest buildHttpRequest(HttpSinkRequestEntry requestEntry, URI endpointUri) {
    return HttpRequest
        .newBuilder()
        .uri(endpointUri)
        .version(java.net.http.HttpClient.Version.HTTP_1_1)
        .header("Content-Type", requestEntry.contentType)
        .method(requestEntry.method, HttpRequest.BodyPublishers.ofByteArray(requestEntry.element))
        .build();
  }

  private CompletableFuture<List<HttpSinkRequestEntryWrapper<Optional<HttpResponse<String>>>>> getCompletedFutures(
      List<HttpSinkRequestEntryWrapper<HttpRequest>> requests
  ) {
    var futures = requests
        .stream()
        .map(req -> httpClient
            .sendAsync(req.element, HttpResponse.BodyHandlers.ofString())
            .exceptionally(ex -> null)
            .thenApply(res -> new HttpSinkRequestEntryWrapper<>(Optional.ofNullable(res), req.sinkRequestEntry))
        )
        .collect(Collectors.toList());

    var allFutures = CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));
    return allFutures.thenApply(_void -> futures.stream().map(CompletableFuture::join).collect(Collectors.toList()));
  }

  private SinkHttpClientResponse prepareHttpClientResponse(
      List<HttpSinkRequestEntryWrapper<Optional<HttpResponse<String>>>> responses
  ) {
    var successfulResponses = new ArrayList<HttpSinkRequestEntry>();
    var failedResponses = new ArrayList<HttpSinkRequestEntry>();

    for (var response : responses) {
      if (response.element.isEmpty() || response.element.get().statusCode() >= 500) {  // TODO: what about 4xx?
        failedResponses.add(response.sinkRequestEntry);
      } else {
        successfulResponses.add(response.sinkRequestEntry);
      }
    }

    return new SinkHttpClientResponse(successfulResponses, failedResponses);
  }
}
