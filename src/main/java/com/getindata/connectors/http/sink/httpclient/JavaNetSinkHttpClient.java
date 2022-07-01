package com.getindata.connectors.http.sink.httpclient;

import com.getindata.connectors.http.SinkHttpClient;
import com.getindata.connectors.http.SinkHttpClientResponse;
import com.getindata.connectors.http.sink.HttpSinkRequestEntry;
import lombok.extern.slf4j.Slf4j;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/**
 * An implementation of {@link SinkHttpClient} that uses Java 11's {@link HttpClient}.
 */
@Slf4j
public class JavaNetSinkHttpClient implements SinkHttpClient {
  private final HttpClient httpClient;

  public JavaNetSinkHttpClient() {
    this.httpClient = HttpClient.newBuilder().followRedirects(HttpClient.Redirect.NORMAL).build();
  }

  @Override
  public CompletableFuture<SinkHttpClientResponse> putRequests(
      List<HttpSinkRequestEntry> requestEntries, String endpointUrl
  ) {
    return submitRequests(requestEntries, endpointUrl).thenApply(this::prepareSinkHttpClientResponse);
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

  private CompletableFuture<List<JavaNetHttpResponseWrapper>> submitRequests(
      List<HttpSinkRequestEntry> requestEntries, String endpointUrl
  ) {
    var endpointUri = URI.create(endpointUrl);
    var responseFutures = new ArrayList<CompletableFuture<JavaNetHttpResponseWrapper>>();

    for (var entry : requestEntries) {
      var response = httpClient
          .sendAsync(buildHttpRequest(entry, endpointUri), HttpResponse.BodyHandlers.ofString())
          .exceptionally(ex -> {
            log.error("Request fatally failed because of an exception", ex);
            return null;
          })
          .thenApply(res -> new JavaNetHttpResponseWrapper(entry, res));
      responseFutures.add(response);
    }

    var allFutures = CompletableFuture.allOf(responseFutures.toArray(new CompletableFuture[0]));
    return allFutures.thenApply(_void -> responseFutures.stream().map(CompletableFuture::join).collect(Collectors.toList()));
  }

  private SinkHttpClientResponse prepareSinkHttpClientResponse(List<JavaNetHttpResponseWrapper> responses) {
    var successfulResponses = new ArrayList<HttpSinkRequestEntry>();
    var failedResponses = new ArrayList<HttpSinkRequestEntry>();

    for (var response : responses) {
      var sinkRequestEntry = response.getSinkRequestEntry();
      if (response.getResponse().isEmpty() || response.getResponse().get().statusCode() >= 400) {
        failedResponses.add(sinkRequestEntry);
      } else {
        successfulResponses.add(sinkRequestEntry);
      }
    }

    return new SinkHttpClientResponse(successfulResponses, failedResponses);
  }
}
