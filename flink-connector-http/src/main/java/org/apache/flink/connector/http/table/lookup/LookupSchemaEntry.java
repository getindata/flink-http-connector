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

import org.apache.flink.connector.http.LookupArg;

import java.io.Serializable;
import java.util.List;

/**
 * Represents Lookup entry with its name and provides conversion method to collection of {@link
 * LookupArg} elements.
 *
 * @param <T> type of lookupKeyRow used for converting to {@link LookupArg}.
 */
public interface LookupSchemaEntry<T> extends Serializable {

    /** @return lookup Field name. */
    String getFieldName();

    /**
     * Creates a collection of {@link LookupArg} elements from provided T lookupKeyRow.
     *
     * @param lookupKeyRow Element to get the values from for {@code LookupArg#getArgValue()}.
     * @return Collection of {@link LookupArg} objects created from lookupKeyRow.
     */
    List<LookupArg> convertToLookupArg(T lookupKeyRow);
}
