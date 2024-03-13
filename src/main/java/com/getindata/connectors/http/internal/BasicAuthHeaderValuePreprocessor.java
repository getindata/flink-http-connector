package com.getindata.connectors.http.internal;

import java.util.Base64;
import java.util.Objects;

/**
 * Header processor for HTTP Basic Authentication mechanism.
 * Only "Basic" authentication is supported currently.
 */
public class BasicAuthHeaderValuePreprocessor implements HeaderValuePreprocessor {

    public static final String BASIC = "Basic ";

    private boolean useRawAuthHeader = false;

    /**
     * Creates a new instance of BasicAuthHeaderValuePreprocessor that uses
     * the default processing of the Authorization header.
     */
    public BasicAuthHeaderValuePreprocessor() {
        this(false);
    }

    /**
     * Creates a new instance of BasicAuthHeaderValuePreprocessor.
     *
     * @param useRawAuthHeader If set to true, the Authorization header is kept as-is,
     *                         untransformed. Otherwise, uses the default processing of the
     *                         Authorization header.
     */
    public BasicAuthHeaderValuePreprocessor(boolean useRawAuthHeader) {
        this.useRawAuthHeader = useRawAuthHeader;
    }

    /**
     * Calculates {@link Base64} value of provided header value. For Basic authentication mechanism,
     * the raw value is expected to match user:password pattern.
     * <p>
     * If rawValue starts with "Basic " prefix, or useRawAuthHeader has been set to true, it is
     * assumed that this value is already converted to the expected "Authorization" header value.
     *
     * @param rawValue header original value to modify.
     * @return value of "Authorization" header with format "Basic " + Base64 from rawValue or
     * rawValue without any changes if it starts with "Basic " prefix or useRawAuthHeader is
     * set to true.
     */
    @Override
    public String preprocessHeaderValue(String rawValue) {
        Objects.requireNonNull(rawValue);
        if (useRawAuthHeader || rawValue.startsWith(BASIC)) {
            return rawValue;
        } else {
            return BASIC + Base64.getEncoder().encodeToString(rawValue.getBytes());
        }
    }
}
