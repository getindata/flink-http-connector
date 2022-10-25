package com.getindata.connectors.http.internal.table.lookup;

import java.io.Serializable;
import java.net.http.HttpRequest;

import org.apache.flink.table.data.RowData;

import com.getindata.connectors.http.LookupArg;

/**
 * Factory for creating {@link HttpRequest} objects for Rest clients.
 */
public interface HttpRequestFactory extends Serializable {

    /**
     * Creates {@link HttpRequest} from given List of {@link LookupArg} objects.
     *
     * @param lookupRow {@link LookupArg} objects used for building http request.
     * @return {@link HttpRequest} created from {@link LookupArg}
     */
    HttpRequest buildLookupRequest(RowData lookupRow);
}
