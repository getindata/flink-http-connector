package com.getindata.connectors.http.internal.status;

/**
 * Describes HttpResponse status, whether it is successful or not. In case of error,
 * it also indicates if, according to configuration, the request can be retried.
 */
public enum HttpResponseStatus {
    /**
     * Successful request.
     */
    SUCCESS,
    /**
     * Request failed but can be retried.
     */
    FAILURE_RETRYABLE,
    /**
     * Request failed but cannot be retried.
     */
    FAILURE_NON_RETRYABLE,
}
