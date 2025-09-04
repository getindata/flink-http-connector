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

package org.apache.flink.connector.http.preprocessor;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * This implementation of {@link HeaderPreprocessor} acts as a registry for all {@link
 * HeaderValuePreprocessor} that should be applied on HTTP request.
 */
public class ComposeHeaderPreprocessor implements HeaderPreprocessor {

    /**
     * Default, pass through header value preprocessor used whenever dedicated preprocessor for a
     * given header does not exist.
     */
    private static final HeaderValuePreprocessor DEFAULT_VALUE_PREPROCESSOR = rawValue -> rawValue;

    /** Map with {@link HeaderValuePreprocessor} to apply. */
    private final Map<String, HeaderValuePreprocessor> valuePreprocessors;

    /**
     * Creates a new instance of ComposeHeaderPreprocessor for provided {@link
     * HeaderValuePreprocessor} map.
     *
     * @param valuePreprocessors map of {@link HeaderValuePreprocessor} that should be used for this
     *     processor. If null, then default, pass through header value processor will be used for
     *     every header.
     */
    public ComposeHeaderPreprocessor(Map<String, HeaderValuePreprocessor> valuePreprocessors) {
        this.valuePreprocessors =
                (valuePreprocessors == null)
                        ? Collections.emptyMap()
                        : new HashMap<>(valuePreprocessors);
    }

    @Override
    public String preprocessValueForHeader(String headerName, String headerRawValue) {
        return valuePreprocessors
                .getOrDefault(headerName, DEFAULT_VALUE_PREPROCESSOR)
                .preprocessHeaderValue(headerRawValue);
    }
}
