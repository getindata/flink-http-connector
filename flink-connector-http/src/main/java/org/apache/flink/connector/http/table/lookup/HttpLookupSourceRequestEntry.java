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

package org.apache.flink.connector.http.table.lookup;

import lombok.Data;
import lombok.ToString;

import java.net.http.HttpRequest;

/**
 * Wrapper class around {@link HttpRequest} that contains information about an actual lookup request
 * body or request parameters.
 */
@Data
@ToString
public class HttpLookupSourceRequestEntry {

    /** Wrapped {@link HttpRequest} object. */
    private final HttpRequest httpRequest;

    /**
     * This field represents lookup query. Depending on used REST request method, this field can
     * represent a request body, for example a Json string when PUT/POST requests method was used,
     * or it can represent a query parameters if GET method was used.
     */
    private final LookupQueryInfo lookupQueryInfo;

    public HttpLookupSourceRequestEntry(HttpRequest httpRequest, LookupQueryInfo lookupQueryInfo) {
        this.httpRequest = httpRequest;
        this.lookupQueryInfo = lookupQueryInfo;
    }
}
