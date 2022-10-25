package com.getindata.connectors.http;

import lombok.Data;
import lombok.RequiredArgsConstructor;

/**
 * Transfer object that contains single lookup argument (column name) and its value.
 */
@Data
@RequiredArgsConstructor
public class LookupArg {

    /**
     * Lookup argument name.
     */
    private final String argName;

    /**
     * Lookup argument value.
     */
    private final String argValue;
}
