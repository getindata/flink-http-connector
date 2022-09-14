package com.getindata.connectors.http.internal;

import java.util.Base64;

/**
 * Header processor for HTTP Basic Authentication mechanism.
 * Only "Basic" authentication is supported currently.
 */
public class BasicAuthHeaderValuePreprocessor implements HeaderValuePreprocessor {

    public static final String BASIC = "Basic ";

    /**
     * Calculates {@link Base64} value of provided header value. For Basic authentication mechanism,
     * the raw value is expected to match user:password pattern.
     * <p>
     * If rawValue starts with "Basic " prefix it is assumed that this value is already converted to
     * expected "Authorization" header value.
     *
     * @param rawValue header original value to modify.
     * @return value of "Authorization" header with format "Basic " + Base64 from rawValue or
     * rawValue without any changes if it starts with "Basic " prefix.
     */
    @Override
    public String preprocessHeaderValue(String rawValue) {
        if (rawValue.startsWith(BASIC)) {
            return rawValue;
        } else {
            return BASIC + Base64.getEncoder().encodeToString(rawValue.getBytes());
        }
    }
}
