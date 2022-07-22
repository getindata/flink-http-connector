package com.getindata.connectors.http.internal.sink.httpclient.status;

/**
 * Base interface for all classes that would validate HTTP status
 * code whether it is an error or not.
 */
public interface HttpStatusCodeChecker {

    /**
     * Validates http status code wheter it is considered as error code. The logic for
     * what status codes are considered as "errors" depends on the concreted implementation
     * @param statusCode http status code to assess.
     * @return true if statusCode is considered as Error and false if not.
     */
    boolean isErrorCode(int statusCode);
}
