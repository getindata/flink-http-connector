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

package org.apache.flink.connector.http.clients;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.FunctionContext;

import java.util.Collection;

/** A client that is used to get enrichment data from external component. */
public interface PollingClient<T> {

    /**
     * Gets enrichment data from external component using provided lookup arguments.
     *
     * @param lookupRow A {@link RowData} containing request parameters.
     * @return an optional result of data lookup.
     */
    Collection<T> pull(RowData lookupRow);

    /**
     * Initialize the client.
     *
     * @param ctx function context
     */
    void open(FunctionContext ctx);
}
