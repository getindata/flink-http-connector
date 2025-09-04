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

package org.apache.flink.connector.http;

import org.apache.flink.connector.http.table.lookup.LookupQueryInfo;
import org.apache.flink.table.data.RowData;

import java.io.Serializable;

/**
 * An interface for a creator of a lookup query in the Http Lookup Source (e.g., the query that gets
 * appended as query parameters to the URI in GET request or supplied as the payload of a body-based
 * request along with optional query parameters).
 *
 * <p>One can customize how those queries are built by implementing {@link LookupQueryCreator} and
 * {@link LookupQueryCreatorFactory}.
 */
public interface LookupQueryCreator extends Serializable {

    /**
     * Create a lookup query (like the query appended to path in GET request) out of the provided
     * arguments.
     *
     * @param lookupDataRow a {@link RowData} containing request parameters.
     * @return a lookup query.
     */
    LookupQueryInfo createLookupQuery(RowData lookupDataRow);
}
