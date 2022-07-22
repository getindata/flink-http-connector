package com.getindata.connectors.http.internal.sink.httpclient.status;

import lombok.EqualsAndHashCode;
import lombok.RequiredArgsConstructor;

/**
 * An implementation of {@link HttpStatusCodeChecker} that validates status code against
 * constant value.
 */
@RequiredArgsConstructor
@EqualsAndHashCode
public class SingleValueHttpStatusCodeChecker implements HttpStatusCodeChecker {

    /**
     * A reference http status code to compare with.
     */
    private final int errorCode;

    /**
     * Validates given statusCode against constant value.
     * @param statusCode http status code to assess.
     * @return true if status code is considered as error or false if not.
     */
    @Override
    public boolean isErrorCode(int statusCode) {
        return errorCode == statusCode;
    }
}
