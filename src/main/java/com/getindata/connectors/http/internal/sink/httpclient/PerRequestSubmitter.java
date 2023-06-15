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
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;

import com.getindata.connectors.http.internal.config.HttpConnectorConfigConstants;
import com.getindata.connectors.http.internal.sink.HttpSinkRequestEntry;
import com.getindata.connectors.http.internal.utils.JavaNetHttpClientFactory;
import com.getindata.connectors.http.internal.utils.ThreadUtils;

@Slf4j
public class PerRequestSubmitter implements RequestSubmitter {

    private static final int HTTP_CLIENT_THREAD_POOL_SIZE = 16;

    public static final String DEFAULT_REQUEST_TIMEOUT_SECONDS = "30";

    /**
     * Thread pool to handle HTTP response from HTTP client.
     */
    private final ExecutorService publishingThreadPool;

    private final int httpRequestTimeOutSeconds;

    private final String[] headersAndValues;

    private final HttpClient httpClient;

    public PerRequestSubmitter(Properties properties, String[] headersAndValues) {

        this.headersAndValues = headersAndValues;

        this.publishingThreadPool =
            Executors.newFixedThreadPool(
                HTTP_CLIENT_THREAD_POOL_SIZE,
                new ExecutorThreadFactory(
                    "http-sink-client-response-worker", ThreadUtils.LOGGING_EXCEPTION_HANDLER));

        this.httpRequestTimeOutSeconds = Integer.parseInt(
            properties.getProperty(HttpConnectorConfigConstants.SINK_HTTP_TIMEOUT_SECONDS,
                DEFAULT_REQUEST_TIMEOUT_SECONDS)
        );

        ExecutorService httpClientExecutor =
            Executors.newFixedThreadPool(
                HTTP_CLIENT_THREAD_POOL_SIZE,
                new ExecutorThreadFactory(
                    "http-sink-client-request-worker", ThreadUtils.LOGGING_EXCEPTION_HANDLER));

        this.httpClient = JavaNetHttpClientFactory.createClient(properties, httpClientExecutor);
    }

    @Override
    public List<CompletableFuture<JavaNetHttpResponseWrapper>> submit(
            String endpointUrl,
            List<HttpSinkRequestEntry> requestToSubmit) {

        var endpointUri = URI.create(endpointUrl);
        var responseFutures = new ArrayList<CompletableFuture<JavaNetHttpResponseWrapper>>();

        for (var entry : requestToSubmit) {
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
        return responseFutures;
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
}
