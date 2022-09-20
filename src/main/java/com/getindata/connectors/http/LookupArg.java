package com.getindata.connectors.http;

import lombok.Data;
import lombok.RequiredArgsConstructor;

/**
 * Transfer object that contains Http request argument and value.
 */
@Data
@RequiredArgsConstructor
public class LookupArg {

    /**
     * HTTP request argument's name.
     */
    private final String argName;

    /**
     * HTTP request argument's value.
     */
    private final String argValue;
}
