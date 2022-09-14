package com.getindata.connectors.http.internal;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * This implementation of {@link HeaderPreprocessor} acts as a registry for all {@link
 * HeaderValuePreprocessor} that should be applied on HTTP request.
 */
public class ComposeHeaderPreprocessor implements HeaderPreprocessor {

    /**
     * Default, pass through header value preprocessor used whenever dedicated preprocessor for a
     * given header does not exist.
     */
    private static final HeaderValuePreprocessor DEFAULT_VALUE_PREPROCESSOR = rawValue -> rawValue;

    /**
     * Map with {@link HeaderValuePreprocessor} to apply.
     */
    private final Map<String, HeaderValuePreprocessor> valuePreprocessors;

    /**
     * Creates a new instance of ComposeHeaderPreprocessor for provided {@link
     * HeaderValuePreprocessor} map.
     *
     * @param valuePreprocessors map of {@link HeaderValuePreprocessor} that should be used for this
     *                           processor. If null, then default, pass through header value
     *                           processor will be used for every header.
     */
    public ComposeHeaderPreprocessor(Map<String, HeaderValuePreprocessor> valuePreprocessors) {
        this.valuePreprocessors = (valuePreprocessors == null)
            ? Collections.emptyMap()
            : new HashMap<>(valuePreprocessors);
    }

    @Override
    public String preprocessValueForHeader(String headerName, String headerRawValue) {
        return valuePreprocessors
            .getOrDefault(headerName, DEFAULT_VALUE_PREPROCESSOR)
            .preprocessHeaderValue(headerRawValue);
    }
}
