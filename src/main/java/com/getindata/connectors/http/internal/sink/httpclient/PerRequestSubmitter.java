package com.getindata.connectors.http.internal.sink.httpclient;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;

import com.getindata.connectors.http.internal.utils.HttpClientUtils;
import com.google.common.collect.Lists;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import com.getindata.connectors.http.internal.sink.HttpSinkRequestEntry;
import okhttp3.*;

/**
 * This implementation creates HTTP requests for every processed event.
 */
@Slf4j
public class PerRequestSubmitter extends AbstractRequestSubmitter {

    public PerRequestSubmitter(
            Properties properties,
            String[] headersAndValues,
            OkHttpClient httpClient) {
        super(properties, headersAndValues, httpClient);
    }

    @Override
    @SneakyThrows
    public List<CompletableFuture<JavaNetHttpResponseWrapper>> submit(
            String endpointUrl,
            List<HttpSinkRequestEntry> requestToSubmit) {

        URI endpointUri = URI.create(endpointUrl);
        List<CompletableFuture<JavaNetHttpResponseWrapper>> responseFutures = new ArrayList<>();

        for (HttpSinkRequestEntry entry : requestToSubmit) {
            HttpRequest httpRequest = buildHttpRequest(entry, endpointUri);
            CompletableFuture<Response> responseFuture = HttpClientUtils.sendAsyncRequest(httpClient, httpRequest.getHttpRequest());

            CompletableFuture<JavaNetHttpResponseWrapper> response = responseFuture.exceptionally(ex -> {
                    // TODO This will be executed on a ForkJoinPool Thread... refactor this someday.
                    log.error("Request fatally failed because of an exception", ex);
                    return null;
                }).thenApplyAsync(
                    res -> new JavaNetHttpResponseWrapper(httpRequest, res),
                    publishingThreadPool
            );
            responseFutures.add(response);
        }
        return responseFutures;
    }

    @SneakyThrows
    private HttpRequest buildHttpRequest(HttpSinkRequestEntry requestEntry, URI endpointUri) {
        Request.Builder requestBuilder = new Request
            .Builder()
            .url(endpointUri.toURL().toString())
//            .version(Version.HTTP_1_1)
//            .timeout(Duration.ofSeconds(httpRequestTimeOutSeconds))
            .method(requestEntry.method, RequestBody.create(requestEntry.element, MediaType.parse("application/json; charset=utf-8")));

        if (headersAndValues.length != 0) {
            requestBuilder.headers(Headers.of(headersAndValues));
        }

        return new HttpRequest(
            requestBuilder.build(),
            Lists.newArrayList(requestEntry.element),
            requestEntry.method
        );
    }
}
