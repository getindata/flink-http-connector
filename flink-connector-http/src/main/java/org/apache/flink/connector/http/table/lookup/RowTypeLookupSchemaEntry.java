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
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.RowData.FieldGetter;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

/**
 * Implementation of {@link LookupSchemaEntry} for {@link RowData} type that represents multiple
 * columns.
 */
public class RowTypeLookupSchemaEntry extends RowDataLookupSchemaEntryBase<RowData> {

    /**
     * {@link LookupSchemaEntry} elements for every lookup column represented by {@link
     * RowTypeLookupSchemaEntry} instance.
     */
    private final List<LookupSchemaEntry<RowData>> keyColumns;

    /**
     * Creates new instance.
     *
     * @param fieldName field name that this instance represents, matching {@link RowData} column
     *     name.
     * @param fieldGetter {@link RowData.FieldGetter} for data type matching {@link RowData} column
     *     type that this instance represents.
     */
    public RowTypeLookupSchemaEntry(String fieldName, FieldGetter fieldGetter) {
        super(fieldName, fieldGetter);
        this.keyColumns = new LinkedList<>();
    }

    /**
     * Add {@link LookupSchemaEntry} to keyColumns that this {@link RowTypeLookupSchemaEntry}
     * represents.
     *
     * @param lookupSchemaEntry {@link LookupSchemaEntry} to add.
     * @return this {@link RowTypeLookupSchemaEntry} instance.
     */
    public RowTypeLookupSchemaEntry addLookupEntry(LookupSchemaEntry<RowData> lookupSchemaEntry) {
        this.keyColumns.add(lookupSchemaEntry);
        return this;
    }

    /**
     * Creates collection of {@link LookupArg} that represents every lookup element from {@link
     * LookupSchemaEntry} added to this instance.
     *
     * @param lookupKeyRow Element to get the values from for {@code LookupArg#getArgValue()}.
     * @return collection of {@link LookupArg} that represents entire lookup {@link RowData}
     */
    @Override
    public List<LookupArg> convertToLookupArg(RowData lookupKeyRow) {

        RowData nestedRow = (RowData) fieldGetter.getFieldOrNull(lookupKeyRow);

        if (nestedRow == null) {
            return Collections.emptyList();
        }

        List<LookupArg> lookupArgs = new LinkedList<>();
        for (LookupSchemaEntry<RowData> lookupSchemaEntry : keyColumns) {
            lookupArgs.addAll(lookupSchemaEntry.convertToLookupArg(nestedRow));
        }

        return lookupArgs;
    }

    @lombok.Generated
    @Override
    public String toString() {
        return "RowTypeLookupSchemaEntry{"
                + "fieldName='"
                + fieldName
                + '\''
                + ", fieldGetter="
                + fieldGetter
                + ", keyColumns="
                + keyColumns
                + '}';
    }
}
