package com.getindata.connectors.http.internal;

import java.io.Serializable;

/**
 * Interface for header preprocessing
 */
public interface HeaderPreprocessor extends Serializable {

    /**
     * Preprocess value of a header.Preprocessing can change or validate header value.
     * @param headerName header name which value should be preprocessed.
     * @param headerRawValue header value to process.
     * @return preprocessed header value.
     */
    String preprocessValueForHeader(String headerName, String headerRawValue);
}
