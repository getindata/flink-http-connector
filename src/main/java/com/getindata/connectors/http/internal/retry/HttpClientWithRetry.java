package com.getindata.connectors.http.internal.retry;

import com.getindata.connectors.http.HttpStatusCodeValidationFailedException;
import com.getindata.connectors.http.internal.status.HttpResponseChecker;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import io.github.resilience4j.retry.RetryRegistry;
import lombok.Builder;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.metrics.MetricGroup;

import java.io.IOException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.function.Supplier;

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
        retryConfig = RetryConfig.from(retryConfig)
                .retryExceptions(IOException.class, RetryHttpRequestException.class)
                .build();
        this.retry = RetryRegistry.ofDefaults().retry("http-lookup-connector", retryConfig);
    }

    public void registerMetrics(MetricGroup metrics){
        var group = metrics.addGroup("http_lookup_connector");
        group.gauge("failed_calls_with_retry_attempt_count",
            () -> retry.getMetrics().getNumberOfFailedCallsWithRetryAttempt());
        group.gauge("failed_calls_without_retry_attempt_count",
            () -> retry.getMetrics().getNumberOfFailedCallsWithoutRetryAttempt());
        group.gauge("success_calls_with_retry_attempt",
            () -> retry.getMetrics().getNumberOfSuccessfulCallsWithRetryAttempt());
        group.gauge("success_calls_without_retry_attempt",
            () -> retry.getMetrics().getNumberOfSuccessfulCallsWithoutRetryAttempt());
    }

    public <T> HttpResponse<T> send(
            Supplier<HttpRequest> requestSupplier,
            HttpResponse.BodyHandler<T> responseBodyHandler
    ) throws IOException, InterruptedException, HttpStatusCodeValidationFailedException {
        try {
            try {
                return Retry.decorateCheckedSupplier(retry, () -> task(requestSupplier, responseBodyHandler)).apply();
            } catch (RetryHttpRequestException retryException) {
                throw retryException.getCausedBy();
            }
        } catch (IOException | InterruptedException | HttpStatusCodeValidationFailedException e) {
            throw e;
        } catch (Throwable t) {
            throw new RuntimeException("Unexpected exception", t);
        }
    }

    private <T> HttpResponse<T> task(
            Supplier<HttpRequest> requestSupplier,
            HttpResponse.BodyHandler<T> responseBodyHandler
    ) throws IOException, InterruptedException, RetryHttpRequestException, HttpStatusCodeValidationFailedException {
        var request = requestSupplier.get();
        var response = httpClient.send(request, responseBodyHandler);
        if (responseChecker.isSuccessful(response)) {
            return response;
        }
        var validationFailedException = new HttpStatusCodeValidationFailedException(
                "Incorrect response code: " + response.statusCode(), response);
        if (responseChecker.isTemporalError(response)) {
            log.debug("Retrying... Received response with code {} for request {}", response.statusCode(), request);
            throw new RetryHttpRequestException(validationFailedException);
        }
        throw validationFailedException;
    }
}

@Getter
@RequiredArgsConstructor
class RetryHttpRequestException extends Exception {
    private final HttpStatusCodeValidationFailedException causedBy;
}
