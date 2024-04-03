package com.getindata.connectors.http.internal.table.lookup;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Map;
import java.util.stream.Collectors;

import lombok.Getter;
import lombok.ToString;

import com.getindata.connectors.http.internal.utils.uri.NameValuePair;
import com.getindata.connectors.http.internal.utils.uri.URLEncodedUtils;


/**
 * Holds the lookup query for an HTTP request.
 * The {@code  lookupQuery} either contain the query parameters for a GET operation
 * or the payload of a body-based request.
 * The {@code bodyBasedUrlQueryParams} contains the optional query parameters of a
 * body-based request in addition to its payload supplied with {@code  lookupQuery}.
 */
@ToString
public class LookupQueryInfo implements Serializable {
    @Getter
    private final String lookupQuery;

    private final Map<String, String> bodyBasedUrlQueryParams;

    private final Map<String, String> pathBasedUrlParams;

    public LookupQueryInfo(String lookupQuery) {
        this(lookupQuery, null, null);
    }

    public LookupQueryInfo(String lookupQuery, Map<String, String> bodyBasedUrlQueryParams,
                           Map<String, String> pathBasedUrlParams) {
        this.lookupQuery =
                lookupQuery == null ? "" : lookupQuery;
        this.bodyBasedUrlQueryParams =
                bodyBasedUrlQueryParams == null ? Collections.emptyMap() : bodyBasedUrlQueryParams;
        this.pathBasedUrlParams =
                pathBasedUrlParams == null ? Collections.emptyMap() : pathBasedUrlParams;
    }

    public String getBodyBasedUrlQueryParameters() {
        return URLEncodedUtils.format(
                bodyBasedUrlQueryParams
                        .entrySet()
                        .stream()
                        // sort the map by key to ensure there is a reliable order for unit tests
                        .sorted(Map.Entry.comparingByKey())
                        .map(entry -> new NameValuePair(entry.getKey(), entry.getValue()))
                        .collect(Collectors.toList()),
                StandardCharsets.UTF_8);
    }

    public Map<String, String> getPathBasedUrlParameters() {
        return pathBasedUrlParams;
    }

    public boolean hasLookupQuery() {
        return !lookupQuery.isBlank();
    }

    public boolean hasBodyBasedUrlQueryParameters() {
        return !bodyBasedUrlQueryParams.isEmpty();
    }

    public boolean hasPathBasedUrlParameters() {
        return !pathBasedUrlParams.isEmpty();
    }
}
