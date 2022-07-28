package com.getindata.connectors.http.internal.sink.httpclient.status;

import lombok.EqualsAndHashCode;

/**
 * Implementation of {@link HttpStatusCodeChecker} that verifies if given Http status code
 * belongs to specific HTTP code type family. For example if it any of 100's 200's or 500's code.
 */
@EqualsAndHashCode
public class TypeStatusCodeChecker implements HttpStatusCodeChecker {

    /**
     * First digit from HTTP status code that describes a type of code,
     * for example 1 for all 100's, 5 for all 500's.
     */
    private final int httpTypeCode;

    /**
     * Creates TypeStatusCodeChecker for given {@link HttpResponseCodeType}
     *
     * @param httpResponseCodeType {@link HttpResponseCodeType} for this {@link
     *                             TypeStatusCodeChecker} instance.
     */
    public TypeStatusCodeChecker(HttpResponseCodeType httpResponseCodeType) {
        this.httpTypeCode = httpResponseCodeType.getHttpTypeCode();
    }

    /**
     * Checks whether given status code belongs to Http code status type.
     * For example:
     * <pre>{@code
     *    TypeStatusCodeChecker checker =  new TypeStatusCodeChecker(5);
     *    checker.isErrorCode(505); <- will return true.
     *    }
     * </pre>
     * @param statusCode http status code to assess.
     * @return true if status code is considered as error or false if not.
     */
    @Override
    public boolean isErrorCode(int statusCode) {
        return statusCode / 100 == httpTypeCode;
    }
}
