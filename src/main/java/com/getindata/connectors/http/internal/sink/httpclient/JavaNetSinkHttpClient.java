package com.getindata.connectors.http.internal.sink.httpclient;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpClient.Version;
import java.net.http.HttpRequest;
import java.net.http.HttpRequest.BodyPublishers;
import java.net.http.HttpRequest.Builder;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;

import com.getindata.connectors.http.HttpPostRequestCallback;
import com.getindata.connectors.http.internal.HeaderPreprocessor;
import com.getindata.connectors.http.internal.SinkHttpClient;
import com.getindata.connectors.http.internal.SinkHttpClientResponse;
import com.getindata.connectors.http.internal.config.HttpConnectorConfigConstants;
import com.getindata.connectors.http.internal.sink.HttpSinkRequestEntry;
import com.getindata.connectors.http.internal.status.ComposeHttpStatusCodeChecker;
import com.getindata.connectors.http.internal.status.ComposeHttpStatusCodeChecker.ComposeHttpStatusCodeCheckerConfig;
import com.getindata.connectors.http.internal.status.HttpStatusCodeChecker;
import com.getindata.connectors.http.internal.table.sink.Slf4jHttpPostRequestCallback;
import com.getindata.connectors.http.internal.utils.HttpHeaderUtils;
import com.getindata.connectors.http.internal.utils.JavaNetHttpClientFactory;
import com.getindata.connectors.http.internal.utils.ThreadUtils;

/**
 * An implementation of {@link SinkHttpClient} that uses Java 11's {@link HttpClient}. This
 * implementation supports HTTP traffic only.
 */
@Slf4j
public class JavaNetSinkHttpClient implements SinkHttpClient {

    private static final int HTTP_CLIENT_THREAD_POOL_SIZE = 16;

    public static final String DEFAULT_REQUEST_TIMEOUT_SECONDS = "30";

    private final HttpClient httpClient;

    private final String[] headersAndValues;

    private final Map<String, String> headerMap;

    private final HttpStatusCodeChecker statusCodeChecker;

    private final HttpPostRequestCallback<HttpSinkRequestEntry> httpPostRequestCallback;

    /**
     * Thread pool to handle HTTP response from HTTP client.
     */
    private final ExecutorService publishingThreadPool;

    private final int httpRequestTimeOutSeconds;

    public JavaNetSinkHttpClient(Properties properties, HeaderPreprocessor headerPreprocessor) {
        this(properties, new Slf4jHttpPostRequestCallback(), headerPreprocessor);
    }

    public JavaNetSinkHttpClient(
        Properties properties,
        HttpPostRequestCallback<HttpSinkRequestEntry> httpPostRequestCallback,
        HeaderPreprocessor headerPreprocessor) {

        ExecutorService httpClientExecutor =
            Executors.newFixedThreadPool(
                HTTP_CLIENT_THREAD_POOL_SIZE,
                new ExecutorThreadFactory(
                    "http-sink-client-request-worker", ThreadUtils.LOGGING_EXCEPTION_HANDLER));

        this.httpClient = JavaNetHttpClientFactory.createClient(properties, httpClientExecutor);
        this.httpPostRequestCallback = httpPostRequestCallback;
        this.headerMap = HttpHeaderUtils.prepareHeaderMap(
            HttpConnectorConfigConstants.SINK_HEADER_PREFIX,
            properties,
            headerPreprocessor
        );
        this.headersAndValues = HttpHeaderUtils.toHeaderAndValueArray(this.headerMap);

        // TODO Inject this via constructor when implementing a response processor.
        //  Processor will be injected and it will wrap statusChecker implementation.
        ComposeHttpStatusCodeCheckerConfig checkerConfig =
            ComposeHttpStatusCodeCheckerConfig.builder()
                .properties(properties)
                .whiteListPrefix(HttpConnectorConfigConstants.HTTP_ERROR_SINK_CODE_WHITE_LIST)
                .errorCodePrefix(HttpConnectorConfigConstants.HTTP_ERROR_SINK_CODES_LIST)
                .build();

        this.statusCodeChecker = new ComposeHttpStatusCodeChecker(checkerConfig);

        this.publishingThreadPool =
            Executors.newFixedThreadPool(
                HTTP_CLIENT_THREAD_POOL_SIZE,
                new ExecutorThreadFactory(
                    "http-sink-client-response-worker", ThreadUtils.LOGGING_EXCEPTION_HANDLER));

        this.httpRequestTimeOutSeconds = Integer.parseInt(
            properties.getProperty(HttpConnectorConfigConstants.SINK_HTTP_TIMEOUT_SECONDS,
                DEFAULT_REQUEST_TIMEOUT_SECONDS)
        );
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
            .timeout(Duration.ofSeconds(httpRequestTimeOutSeconds))
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
                .sendAsync(
                    buildHttpRequest(entry, endpointUri),
                    HttpResponse.BodyHandlers.ofString())
                .exceptionally(ex -> {
                    // TODO This will be executed on a ForJoinPool Thread... refactor this someday.
                    log.error("Request fatally failed because of an exception", ex);
                    return null;
                })
                .thenApplyAsync(
                    res -> new JavaNetHttpResponseWrapper(entry, res),
                    publishingThreadPool
                );
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
