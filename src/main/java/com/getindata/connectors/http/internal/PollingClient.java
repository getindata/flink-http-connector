package com.getindata.connectors.http.internal;

import java.util.Optional;

import com.getindata.connectors.http.internal.table.lookup.HttpLookupSourceRequestEntry;

/**
 * A client that is used to get enrichment data from external component.
 */
public interface PollingClient<T> {

    /**
     * Gets enrichment data from external component using provided lookup arguments.
     * @param request A {@link HttpLookupSourceRequestEntry} request.
     * @return an optional result of data lookup.
     */
    Optional<T> pull(HttpLookupSourceRequestEntry request);
}
