package com.getindata.connectors.http;

/**
 * Exception thrown from a {@link HttpPostRequestCallback} when a request should be considered as failed.
 *
 * <p>This exception is caught by the {@link com.getindata.connectors.http.internal.sink.httpclient.JavaNetSinkHttpClient}
 * and {@link com.getindata.connectors.http.internal.table.lookup.JavaNetHttpPollingClient}
 */
public class FailedRequestException extends Exception {
    public FailedRequestException(String message) {
        super(message);
    }

    public FailedRequestException(String message, Throwable cause) {
        super(message, cause);
    }
}
