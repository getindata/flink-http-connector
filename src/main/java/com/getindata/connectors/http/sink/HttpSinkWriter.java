package com.getindata.connectors.http.sink;

import lombok.EqualsAndHashCode;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.connector.base.sink.writer.AsyncSinkWriter;
import org.apache.flink.connector.base.sink.writer.BufferedRequestState;
import org.apache.flink.connector.base.sink.writer.ElementConverter;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
public class HttpSinkWriter<InputT> extends AsyncSinkWriter<InputT, HttpSinkRequestEntry> {
  private final String endpointUrl;
  private final HttpClient httpClient;

  public HttpSinkWriter(
      ElementConverter<InputT, HttpSinkRequestEntry> elementConverter, Sink.InitContext context, int maxBatchSize,
      int maxInFlightRequests, int maxBufferedRequests, long maxBatchSizeInBytes, long maxTimeInBufferMS,
      long maxRecordSizeInBytes, String endpointUrl
  ) {
    this(elementConverter, context, maxBatchSize, maxInFlightRequests, maxBufferedRequests, maxBatchSizeInBytes,
         maxTimeInBufferMS, maxRecordSizeInBytes, endpointUrl, Collections.emptyList()
    );
  }

  public HttpSinkWriter(
      ElementConverter<InputT, HttpSinkRequestEntry> elementConverter, Sink.InitContext context, int maxBatchSize,
      int maxInFlightRequests, int maxBufferedRequests, long maxBatchSizeInBytes, long maxTimeInBufferMS,
      long maxRecordSizeInBytes, String endpointUrl,
      Collection<BufferedRequestState<HttpSinkRequestEntry>> bufferedRequestStates
  ) {
    super(elementConverter, context, maxBatchSize, maxInFlightRequests, maxBufferedRequests, maxBatchSizeInBytes,
          maxTimeInBufferMS, maxRecordSizeInBytes, bufferedRequestStates
    );
    this.endpointUrl = endpointUrl;
    this.httpClient = HttpClient.newBuilder().followRedirects(HttpClient.Redirect.NORMAL).build();
  }

  @RequiredArgsConstructor
  @EqualsAndHashCode
  private static class HttpSinkRequestEntryPair<T> {
    public final T element;
    public final HttpSinkRequestEntry sinkRequestEntry;
  }

  @Override
  protected void submitRequestEntries(
      List<HttpSinkRequestEntry> requestEntries, Consumer<List<HttpSinkRequestEntry>> requestResult
  ) {
    List<HttpSinkRequestEntryPair<HttpRequest>> requests = requestEntries
        .stream()
        .map(requestEntry -> new HttpSinkRequestEntryPair<>(buildHttpRequest(requestEntry), requestEntry))
        .collect(Collectors.toList());

    var completedFutures = getCompletedFutures(requests);
    var failedRequests = getFailedRequests(completedFutures);

    if (failedRequests.size() > 0) {
      log.error("Http Sink failed to write and will retry {} requests", failedRequests.size());
      requestResult.accept(failedRequests);
    } else {
      requestResult.accept(Collections.emptyList());
    }
  }

  private HttpRequest buildHttpRequest(HttpSinkRequestEntry requestEntry) {
    return HttpRequest
        .newBuilder()
        .uri(URI.create(endpointUrl))
        .version(HttpClient.Version.HTTP_1_1)
        .header("Content-Type", requestEntry.contentType)
        .method(requestEntry.method, HttpRequest.BodyPublishers.ofByteArray(requestEntry.element))
        .build();
  }

  private List<HttpSinkRequestEntryPair<Optional<HttpResponse<String>>>> getCompletedFutures(
      List<HttpSinkRequestEntryPair<HttpRequest>> requests
  ) {
    var futures = requests
        .stream()
        .map(req -> httpClient
            .sendAsync(req.element, HttpResponse.BodyHandlers.ofString())
            .exceptionally(ex -> null)
            .thenApply(res -> new HttpSinkRequestEntryPair<>(Optional.ofNullable(res), req.sinkRequestEntry))
        )
        .collect(Collectors.toList());

    return futures.stream().map(CompletableFuture::join).collect(Collectors.toList());
  }

  private <T> List<HttpSinkRequestEntry> getFailedRequests(
      List<HttpSinkRequestEntryPair<Optional<HttpResponse<T>>>> responsePairs
  ) {
    return responsePairs
        .stream()
        .filter(reqEntryPair -> reqEntryPair.element.isEmpty() || reqEntryPair.element.get().statusCode() >= 500)
        .map(reqEntryPair -> reqEntryPair.sinkRequestEntry)
        .collect(Collectors.toList());
  }

  @Override
  protected long getSizeInBytes(HttpSinkRequestEntry s) {
    return s.getSizeInBytes();
  }
}
