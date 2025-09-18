package com.getindata.connectors.http.internal;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.FunctionContext;

import com.getindata.connectors.http.internal.table.lookup.HttpRowDataWrapper;

/**
 * A client that is used to get enrichment data from external component.
 */
public interface PollingClient {

    /**
     * Gets enrichment data from external component using provided lookup arguments.
     * @param lookupRow A {@link RowData} containing request parameters.
     * @return an optional result of data lookup with http information.
     */
    HttpRowDataWrapper pull(RowData lookupRow);

    /**
     * Initialize the client.
     * @param ctx function context
     */
    void open(FunctionContext ctx);
}
