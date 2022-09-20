package com.getindata.connectors.http.internal.table.lookup.querycreators;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.stream.Collectors;

import com.getindata.connectors.http.LookupArg;
import com.getindata.connectors.http.LookupQueryCreator;
import com.getindata.connectors.http.internal.utils.uri.NameValuePair;
import com.getindata.connectors.http.internal.utils.uri.URLEncodedUtils;

/**
 * A {@link LookupQueryCreator} that builds an "ordinary" GET query, i.e. adds
 * <code>joinColumn1=value1&amp;joinColumn2=value2&amp;...</code> to the URI of the endpoint.
 */
public class GenericGetQueryCreator implements LookupQueryCreator {
    @Override
    public String createLookupQuery(List<LookupArg> params) {
        return URLEncodedUtils.format(
            params.stream()
                  .map(arg -> new NameValuePair(arg.getArgName(), arg.getArgValue()))
                  .collect(Collectors.toList()),
            StandardCharsets.UTF_8);
    }
}
