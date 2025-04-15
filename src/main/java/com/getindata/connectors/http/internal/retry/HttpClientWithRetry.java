package com.getindata.connectors.http.internal.retry;

import java.io.IOException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.function.Supplier;

import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import lombok.Builder;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.metrics.MetricGroup;

import com.getindata.connectors.http.HttpStatusCodeValidationFailedException;
import com.getindata.connectors.http.internal.status.HttpResponseChecker;

@Slf4j
public class HttpClientWithRetry {

    private final HttpClient httpClient;
    @Getter
    private final HttpResponseChecker responseChecker;
    private final Retry retry;

    @Builder
    HttpClientWithRetry(HttpClient httpClient,
                        RetryConfig retryConfig,
                        HttpResponseChecker responseChecker) {
        this.httpClient = httpClient;
        this.responseChecker = responseChecker;
        var adjustedRetryConfig = RetryConfig.from(retryConfig)
                .retryExceptions(IOException.class)
                .retryOnResult(this::isTemporalError)
                .build();
        this.retry = Retry.of("http-lookup-connector", adjustedRetryConfig);
    }

    public void registerMetrics(MetricGroup metrics){
        var group = metrics.addGroup("http_lookup_connector");
        group.gauge("successfulCallsWithRetryAttempt",
            () -> retry.getMetrics().getNumberOfSuccessfulCallsWithRetryAttempt());
        group.gauge("successfulCallsWithoutRetryAttempt",
            () -> retry.getMetrics().getNumberOfSuccessfulCallsWithoutRetryAttempt());
    }

    public <T> HttpResponse<T> send(
            Supplier<HttpRequest> requestSupplier,
            HttpResponse.BodyHandler<T> responseBodyHandler
    ) throws IOException, InterruptedException, HttpStatusCodeValidationFailedException {
        try {
            var response = Retry.decorateCheckedSupplier(retry,
                () -> httpClient.send(requestSupplier.get(), responseBodyHandler)).apply();
            if (!responseChecker.isSuccessful(response)) {
                throw new HttpStatusCodeValidationFailedException(
                        "Incorrect response code: " + response.statusCode(), response);
            }
            return response;
        } catch (IOException | InterruptedException | HttpStatusCodeValidationFailedException e) {
            throw e;    //re-throw without wrapping
        } catch (Throwable t) {
            throw new RuntimeException("Unexpected exception", t);
        }
    }

    private boolean isTemporalError(Object response) {
        return responseChecker.isTemporalError((HttpResponse<?>) response);
    }
}

