package com.getindata.connectors.http.internal.sink.httpclient.status;

import lombok.EqualsAndHashCode;
import lombok.RequiredArgsConstructor;

/**
 * Class that implements logic of a "white list" against single constant value.
 */
@RequiredArgsConstructor
@EqualsAndHashCode
public class WhiteListHttpStatusCodeChecker {

    private final int whiteListCode;

    /**
     * Checks if given statusCode is considered as "white listed"
     * @param statusCode status code to check.
     * @return true if given statusCode is white listed and false if not.
     */
    public boolean isWhiteListed(int statusCode) {
        return whiteListCode == statusCode;
    }
}
