package com.getindata.connectors.http.internal;

import java.util.List;
import java.util.Optional;

import com.getindata.connectors.http.LookupArg;

/**
 * A client that is used to get enrichment data from external component.
 */
public interface PollingClient<T> {

    /**
     * Gets enrichment data from external component using provided lookup arguments.
     * @param lookupArgs The list of {@link LookupArg} containing request parameters.
     * @return an optional result of data lookup.
     */
    Optional<T> pull(List<LookupArg> lookupArgs);
}
