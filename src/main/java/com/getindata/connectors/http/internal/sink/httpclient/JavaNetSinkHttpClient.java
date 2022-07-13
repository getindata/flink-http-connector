package com.getindata.connectors.http.internal.sink.httpclient;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpClient.Version;
import java.net.http.HttpRequest;
import java.net.http.HttpRequest.BodyPublishers;
import java.net.http.HttpRequest.Builder;
import java.net.http.HttpResponse;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.annotation.VisibleForTesting;

import com.getindata.connectors.http.internal.SinkHttpClient;
import com.getindata.connectors.http.internal.SinkHttpClientResponse;
import com.getindata.connectors.http.internal.sink.HttpSinkRequestEntry;
import com.getindata.connectors.http.internal.utils.ConfigUtils;
import static com.getindata.connectors.http.internal.config.HttpConnectorConfigConstants.SINK_HEADER_PREFIX;

/**
 * An implementation of {@link SinkHttpClient} that uses Java 11's {@link HttpClient}.
 * This implementation supports HTTP traffic only.
 */
@Slf4j
public class JavaNetSinkHttpClient implements SinkHttpClient {

    private final HttpClient httpClient;

    private final String[] headersAndValues;

    public JavaNetSinkHttpClient(Properties properties) {
        this.httpClient = HttpClient.newBuilder()
            .followRedirects(HttpClient.Redirect.NORMAL)
            .build();

        Map<String, String> headerMap =
            ConfigUtils.propertiesToMap(properties, SINK_HEADER_PREFIX, String.class);

        headersAndValues = ConfigUtils.flatMapToHeaderArray(headerMap);
    }

    @Override
    public CompletableFuture<SinkHttpClientResponse> putRequests(
            List<HttpSinkRequestEntry> requestEntries,
            String endpointUrl) {
        return submitRequests(requestEntries, endpointUrl).thenApply(
            this::prepareSinkHttpClientResponse);
    }

    private HttpRequest buildHttpRequest(HttpSinkRequestEntry requestEntry, URI endpointUri) {
        Builder requestBuilder = HttpRequest
            .newBuilder()
            .uri(endpointUri)
            .version(Version.HTTP_1_1)
            .method(requestEntry.method,
                BodyPublishers.ofByteArray(requestEntry.element));

        if (headersAndValues.length != 0) {
            requestBuilder.headers(headersAndValues);
        }

        return requestBuilder.build();
    }

    private CompletableFuture<List<JavaNetHttpResponseWrapper>> submitRequests(
            List<HttpSinkRequestEntry> requestEntries,
            String endpointUrl) {
        var endpointUri = URI.create(endpointUrl);
        var responseFutures = new ArrayList<CompletableFuture<JavaNetHttpResponseWrapper>>();

        for (var entry : requestEntries) {
            var response = httpClient
                .sendAsync(buildHttpRequest(entry, endpointUri),
                    HttpResponse.BodyHandlers.ofString())
                .exceptionally(ex -> {
                    log.error("Request fatally failed because of an exception", ex);
                    return null;
                })
                .thenApply(res -> new JavaNetHttpResponseWrapper(entry, res));
            responseFutures.add(response);
        }

        var allFutures = CompletableFuture.allOf(responseFutures.toArray(new CompletableFuture[0]));
        return allFutures.thenApply(_void -> responseFutures.stream().map(CompletableFuture::join)
            .collect(Collectors.toList()));
    }

    private SinkHttpClientResponse prepareSinkHttpClientResponse(
            List<JavaNetHttpResponseWrapper> responses) {
        var successfulResponses = new ArrayList<HttpSinkRequestEntry>();
        var failedResponses = new ArrayList<HttpSinkRequestEntry>();

        for (var response : responses) {
            var sinkRequestEntry = response.getSinkRequestEntry();
            if (response.getResponse().isEmpty()
                || response.getResponse().get().statusCode() >= 400) {
                failedResponses.add(sinkRequestEntry);
            } else {
                successfulResponses.add(sinkRequestEntry);
            }
        }

        return new SinkHttpClientResponse(successfulResponses, failedResponses);
    }

    @VisibleForTesting
    String[] getHeadersAndValues() {
        return Arrays.copyOf(headersAndValues, headersAndValues.length);
    }
}
