/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.http.table.lookup.querycreators;

import org.apache.flink.connector.http.LookupArg;
import org.apache.flink.connector.http.LookupQueryCreator;
import org.apache.flink.connector.http.table.lookup.LookupQueryInfo;
import org.apache.flink.connector.http.table.lookup.LookupRow;
import org.apache.flink.table.data.RowData;

import java.util.Collection;
import java.util.stream.Collectors;

/**
 * A {@link LookupQueryCreator} that prepares <a
 * href="https://www.elastic.co/guide/en/elasticsearch/reference/current/search-search.html#search-api-query-params-q">
 * <code>q</code> parameter GET query</a> for ElasticSearch <i>Search API</i> using Lucene query
 * string syntax (in first versions of the ElasticSearch called <a
 * href="https://www.elastic.co/guide/en/elasticsearch/guide/current/search-lite.html">Search
 * <i>Lite</i></a>).
 */
public class ElasticSearchLiteQueryCreator implements LookupQueryCreator {

    private static final String ENCODED_SPACE = "%20";
    private static final String ENCODED_QUOTATION_MARK = "%22";

    private final LookupRow lookupRow;

    public ElasticSearchLiteQueryCreator(LookupRow lookupRow) {
        this.lookupRow = lookupRow;
    }

    private static String processLookupArg(LookupArg arg) {
        return arg.getArgName()
                + ":"
                + ENCODED_QUOTATION_MARK
                + arg.getArgValue()
                + ENCODED_QUOTATION_MARK;
    }

    @Override
    public LookupQueryInfo createLookupQuery(RowData lookupDataRow) {
        Collection<LookupArg> lookupArgs = lookupRow.convertToLookupArgs(lookupDataRow);

        var luceneQuery =
                lookupArgs.stream()
                        .map(ElasticSearchLiteQueryCreator::processLookupArg)
                        .collect(Collectors.joining(ENCODED_SPACE + "AND" + ENCODED_SPACE));

        String lookupQuery = luceneQuery.isEmpty() ? "" : ("q=" + luceneQuery);

        return new LookupQueryInfo(lookupQuery);
    }
}
