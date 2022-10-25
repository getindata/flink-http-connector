package com.getindata.connectors.http.internal;

import java.util.Optional;

import org.apache.flink.table.data.RowData;

import com.getindata.connectors.http.LookupArg;

/**
 * A client that is used to get enrichment data from external component.
 */
public interface PollingClient<T> {

    /**
     * Gets enrichment data from external component using provided lookup arguments.
     * @param lookupRow The list of {@link LookupArg} containing request parameters.
     * @return an optional result of data lookup.
     */
    Optional<T> pull(RowData lookupRow);
}
