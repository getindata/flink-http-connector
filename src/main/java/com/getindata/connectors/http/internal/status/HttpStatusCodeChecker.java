package com.getindata.connectors.http.internal.status;

/**
 * Base interface for all classes that would validate HTTP status
 * code whether it is a success, an retryable error or not retryable error.
 */
public interface HttpStatusCodeChecker {

    /**
     * Validates http status code whether it is considered as an error code. The logic for
     * what status codes are considered as "errors" depends on the concrete implementation.
     *
     * @param statusCode http status code to assess.
     * @return <code>SUCCESS</code> if statusCode is considered as success,
     * <code>FAILURE_RETRYABLE</code> if the status code indicates transient error,
     * otherwise <code>FAILURE_NON_RETRYABLE</code>.
     */
    HttpResponseStatus checkStatus(int statusCode);
}
