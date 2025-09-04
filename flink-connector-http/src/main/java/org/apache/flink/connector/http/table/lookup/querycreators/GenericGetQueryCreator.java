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
import org.apache.flink.connector.http.utils.uri.NameValuePair;
import org.apache.flink.connector.http.utils.uri.URLEncodedUtils;
import org.apache.flink.table.data.RowData;

import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.stream.Collectors;

/**
 * A {@link LookupQueryCreator} that builds an "ordinary" GET query, i.e. adds <code>
 * joinColumn1=value1&amp;joinColumn2=value2&amp;...</code> to the URI of the endpoint.
 */
public class GenericGetQueryCreator implements LookupQueryCreator {

    private final LookupRow lookupRow;

    public GenericGetQueryCreator(LookupRow lookupRow) {
        this.lookupRow = lookupRow;
    }

    @Override
    public LookupQueryInfo createLookupQuery(RowData lookupDataRow) {

        Collection<LookupArg> lookupArgs = lookupRow.convertToLookupArgs(lookupDataRow);

        String lookupQuery =
                URLEncodedUtils.format(
                        lookupArgs.stream()
                                .map(arg -> new NameValuePair(arg.getArgName(), arg.getArgValue()))
                                .collect(Collectors.toList()),
                        StandardCharsets.UTF_8);

        return new LookupQueryInfo(lookupQuery);
    }
}
