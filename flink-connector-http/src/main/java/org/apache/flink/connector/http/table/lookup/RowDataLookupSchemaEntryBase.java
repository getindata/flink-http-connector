/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.flink.connector.http.table.lookup;

import org.apache.flink.table.data.RowData;

/** Base implementation of {@link LookupSchemaEntry} for {@link RowData} type. */
public abstract class RowDataLookupSchemaEntryBase<T> implements LookupSchemaEntry<RowData> {

    /** Lookup field name represented by this instance. */
    protected final String fieldName;

    /** {@link RowData.FieldGetter} matching RowData type for field represented by this instance. */
    protected final RowData.FieldGetter fieldGetter;

    /**
     * Creates new instance.
     *
     * @param fieldName field name that this instance represents, matching {@link RowData} column
     *     name.
     * @param fieldGetter {@link RowData.FieldGetter} for data type matching {@link RowData} column
     *     type that this instance represents.
     */
    public RowDataLookupSchemaEntryBase(String fieldName, RowData.FieldGetter fieldGetter) {
        this.fieldName = fieldName;
        this.fieldGetter = fieldGetter;
    }

    public String getFieldName() {
        return this.fieldName;
    }
}
