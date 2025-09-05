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

package org.apache.flink.connector.http.status;

import java.util.HashMap;
import java.util.Map;

/** This enum represents HTTP response code types, grouped by "hundreds" digit. */
public enum HttpResponseCodeType {
    INFO(1),
    SUCCESS(2),
    REDIRECTION(3),
    CLIENT_ERROR(4),
    SERVER_ERROR(5);

    private static final Map<Integer, HttpResponseCodeType> map;

    static {
        map = new HashMap<>();
        for (HttpResponseCodeType httpResponseCodeType : HttpResponseCodeType.values()) {
            map.put(httpResponseCodeType.httpTypeCode, httpResponseCodeType);
        }
    }

    private final int httpTypeCode;

    HttpResponseCodeType(int httpTypeCode) {
        this.httpTypeCode = httpTypeCode;
    }

    /**
     * @param statusCode Http status code to get the {@link HttpResponseCodeType} instance for.
     * @return a {@link HttpResponseCodeType} instance based on http type code, for example {@code
     *     HttpResponseCodeType.getByCode(1)} will return {@link HttpResponseCodeType#INFO} type.
     */
    public static HttpResponseCodeType getByCode(int statusCode) {
        return map.get(statusCode);
    }

    /**
     * @return a "hundreds" digit that represents given {@link HttpResponseCodeType} instance. For
     *     example {@code HttpResponseCodeType.INFO.getHttpTypeCode()} will return 1 since HTTP
     *     information repossess have status codes in range 100 - 199.
     */
    public int getHttpTypeCode() {
        return this.httpTypeCode;
    }
}
