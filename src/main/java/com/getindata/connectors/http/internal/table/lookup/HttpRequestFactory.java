package com.getindata.connectors.http.internal.table.lookup;

import java.io.Serializable;
import java.net.http.HttpRequest;
import java.util.List;

import com.getindata.connectors.http.LookupArg;

/**
 * Factory for creating {@link HttpRequest} objects for Rest clients.
 */
public interface HttpRequestFactory extends Serializable {

    /**
     * Creates {@link HttpRequest} from given List of {@link LookupArg} objects.
     *
     * @param params {@link LookupArg} objects used for building http request.
     * @return {@link HttpRequest} created from {@link LookupArg}
     */
    HttpRequest buildLookupRequest(List<LookupArg> params);
}
