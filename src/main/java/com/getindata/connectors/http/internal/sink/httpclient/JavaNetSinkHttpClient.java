package com.getindata.connectors.http.internal.sink.httpclient;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpClient.Version;
import java.net.http.HttpRequest;
import java.net.http.HttpRequest.BodyPublishers;
import java.net.http.HttpRequest.Builder;
import java.net.http.HttpResponse;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.annotation.VisibleForTesting;

import com.getindata.connectors.http.HttpPostRequestCallback;
import com.getindata.connectors.http.internal.SinkHttpClient;
import com.getindata.connectors.http.internal.SinkHttpClientResponse;
import com.getindata.connectors.http.internal.config.HttpConnectorConfigConstants;
import com.getindata.connectors.http.internal.sink.HttpSinkRequestEntry;
import com.getindata.connectors.http.internal.status.ComposeHttpStatusCodeChecker;
import com.getindata.connectors.http.internal.status.ComposeHttpStatusCodeChecker.ComposeHttpStatusCodeCheckerConfig;
import com.getindata.connectors.http.internal.status.HttpStatusCodeChecker;
import com.getindata.connectors.http.internal.utils.ConfigUtils;
import static com.getindata.connectors.http.internal.config.HttpConnectorConfigConstants.SINK_HEADER_PREFIX;

/**
 * An implementation of {@link SinkHttpClient} that uses Java 11's {@link HttpClient}.
 * This implementation supports HTTP traffic only.
 */
@Slf4j
public class JavaNetSinkHttpClient implements SinkHttpClient {

    private final HttpClient httpClient;

    private final Map<String, String> headerMap;

    private final String[] headersAndValues;

    private final HttpStatusCodeChecker statusCodeChecker;

    private final HttpPostRequestCallback<HttpSinkRequestEntry> httpPostRequestCallback;

    public JavaNetSinkHttpClient(Properties properties) {
        this(properties, null);
    }

    public JavaNetSinkHttpClient(
        Properties properties, HttpPostRequestCallback<HttpSinkRequestEntry> httpPostRequestCallback
    ) {
        this.httpClient = HttpClient.newBuilder()
            .followRedirects(HttpClient.Redirect.NORMAL)
            .build();
        this.httpPostRequestCallback = httpPostRequestCallback;

        var propertiesHeaderMap =
            ConfigUtils.propertiesToMap(properties, SINK_HEADER_PREFIX, String.class);
        this.headersAndValues = ConfigUtils.toHeaderAndValueArray(propertiesHeaderMap);
        this.headerMap = new HashMap<>();
        propertiesHeaderMap
            .forEach((key, value) ->
                         this.headerMap.put(ConfigUtils.extractPropertyLastElement(key), value));

        // TODO Inject this via constructor when implementing a response processor.
        //  Processor will be injected and it will wrap statusChecker implementation.
        ComposeHttpStatusCodeCheckerConfig checkerConfig =
            ComposeHttpStatusCodeCheckerConfig.builder()
                .properties(properties)
                .whiteListPrefix(HttpConnectorConfigConstants.HTTP_ERROR_SINK_CODE_WHITE_LIST)
                .errorCodePrefix(HttpConnectorConfigConstants.HTTP_ERROR_SINK_CODES_LIST)
                .build();

        this.statusCodeChecker = new ComposeHttpStatusCodeChecker(checkerConfig);
    }

    @Override
    public CompletableFuture<SinkHttpClientResponse> putRequests(
            List<HttpSinkRequestEntry> requestEntries,
            String endpointUrl) {
        return submitRequests(requestEntries, endpointUrl)
            .thenApply(responses -> prepareSinkHttpClientResponse(responses, endpointUrl));
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
            List<JavaNetHttpResponseWrapper> responses,
            String endpointUrl) {
        var successfulResponses = new ArrayList<HttpSinkRequestEntry>();
        var failedResponses = new ArrayList<HttpSinkRequestEntry>();

        for (var response : responses) {
            var sinkRequestEntry = response.getSinkRequestEntry();
            var optResponse = response.getResponse();

            httpPostRequestCallback.call(
                optResponse.orElse(null), sinkRequestEntry, endpointUrl, headerMap);

            // TODO Add response processor here and orchestrate it with statusCodeChecker.
            if (optResponse.isEmpty() ||
                statusCodeChecker.isErrorCode(optResponse.get().statusCode())) {
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
