package com.getindata.connectors.http.internal;

import java.util.Collection;

import org.apache.flink.table.data.RowData;

/**
 * A client that is used to get enrichment data from external component.
 */
public interface PollingClient<T> {

    /**
     * Gets enrichment data from external component using provided lookup arguments.
     * @param lookupRow A {@link RowData} containing request parameters.
     * @return an optional result of data lookup.
     */
    Collection<T> pull(RowData lookupRow);
}
