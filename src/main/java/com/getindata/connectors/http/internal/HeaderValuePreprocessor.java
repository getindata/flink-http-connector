package com.getindata.connectors.http.internal;

import java.io.Serializable;

/**
 * Processor interface which modifies header value based on implemented logic.
 * An example would be calculation of Value of Authorization header.
 */
public interface HeaderValuePreprocessor extends Serializable {

    /**
     * Modifies header rawValue according to the implemented logic.
     * @param rawValue header original value to modify
     * @return modified header value.
     */
    String preprocessHeaderValue(String rawValue);

}
