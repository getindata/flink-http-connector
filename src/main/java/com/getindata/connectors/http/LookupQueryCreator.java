package com.getindata.connectors.http;

import java.io.Serializable;
import java.util.List;

/**
 * An interface for a creator of a lookup query in the Http Lookup Source (e.g., the query that
 * gets appended to the URI in GET request).
 *
 * <p>One can customize how those queries are built by implementing {@link LookupQueryCreator} and
 * {@link LookupQueryCreatorFactory}.
 */
public interface LookupQueryCreator extends Serializable {
    /**
     * Create a lookup query (like the query appended to path in GET request)
     * out of the provided arguments.
     *
     * @param params the list of {@link LookupArg} containing request parameters.
     * @return a lookup query.
     */
    String createLookupQuery(List<LookupArg> params);
}
