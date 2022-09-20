package com.getindata.connectors.http.internal.table.lookup.querycreators;

import java.util.List;
import java.util.stream.Collectors;

import com.getindata.connectors.http.LookupArg;
import com.getindata.connectors.http.LookupQueryCreator;

/**
 * A {@link LookupQueryCreator} that prepares <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/search-search.html#search-api-query-params-q"><code>q</code> parameter query</a>
 * for ElasticSearch <i>Search API</i> using Lucene query string syntax (in first versions of the
 * ElasticSearch called <a href="https://www.elastic.co/guide/en/elasticsearch/guide/current/search-lite.html">Search <i>Lite</i></a>).
 */
public class ElasticSearchLiteQueryCreator implements LookupQueryCreator {
    private static final String ENCODED_SPACE = "%20";
    private static final String ENCODED_QUOTATION_MARK = "%22";

    @Override
    public String createLookupQuery(List<LookupArg> params) {
        var luceneQuery = params.stream()
            .map(ElasticSearchLiteQueryCreator::processLookupArg)
            .collect(Collectors.joining(ENCODED_SPACE + "AND" + ENCODED_SPACE));

        return luceneQuery.isEmpty() ? "" : ("q=" + luceneQuery);
    }

    private static String processLookupArg(LookupArg arg) {
        return arg.getArgName()
               + ":"
               + ENCODED_QUOTATION_MARK
               + arg.getArgValue()
               + ENCODED_QUOTATION_MARK;
    }
}
