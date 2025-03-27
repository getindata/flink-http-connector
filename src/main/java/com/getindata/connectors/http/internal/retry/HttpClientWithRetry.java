package com.getindata.connectors.http.internal.retry;

import com.getindata.connectors.http.HttpStatusCodeValidationFailedException;
import com.getindata.connectors.http.internal.status.HttpResponseChecker;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.function.Supplier;

@Slf4j
public class HttpClientWithRetry {

    private final HttpClient httpClient;
    private final RetryConfig retryConfig;
    @Getter
    private final HttpResponseChecker responseChecker;

    @Builder
    HttpClientWithRetry(@NonNull HttpClient httpClient,
                        @NonNull RetryConfig retryConfig,
                        @NonNull HttpResponseChecker responseChecker) {
        this.httpClient = httpClient;
        this.responseChecker = responseChecker;
        this.retryConfig = RetryConfig.from(retryConfig)
                .retryExceptions(IOException.class, RetryHttpRequestException.class)
                .build();
    }

    public <T> HttpResponse<T> send(
            Supplier<HttpRequest> requestSupplier,
            HttpResponse.BodyHandler<T> responseBodyHandler
    ) throws IOException, InterruptedException, HttpStatusCodeValidationFailedException {
        var retry = Retry.of("http-lookup-connector", retryConfig);
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
