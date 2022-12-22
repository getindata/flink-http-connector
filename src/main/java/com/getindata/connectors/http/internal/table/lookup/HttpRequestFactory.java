package com.getindata.connectors.http.internal.table.lookup;

import java.io.Serializable;
import java.net.http.HttpRequest;

import org.apache.flink.table.data.RowData;

/**
 * Factory for creating {@link HttpRequest} objects for Rest clients.
 */
public interface HttpRequestFactory extends Serializable {

    /**
     * Creates a {@link HttpRequest} from given {@link RowData}.
     *
     * @param lookupRow {@link RowData} object used for building http request.
     * @return {@link HttpRequest} created from {@link RowData}
     */
    HttpLookupSourceRequestEntry buildLookupRequest(RowData lookupRow);
}
