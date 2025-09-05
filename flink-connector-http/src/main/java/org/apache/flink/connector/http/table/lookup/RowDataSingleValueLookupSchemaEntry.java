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
import org.apache.flink.table.data.binary.BinaryStringData;

import lombok.extern.slf4j.Slf4j;

import java.util.Collections;
import java.util.List;

/**
 * Implementation of {@link LookupSchemaEntry} for {@link RowData} type that represents single
 * lookup column.
 */
@Slf4j
public class RowDataSingleValueLookupSchemaEntry extends RowDataLookupSchemaEntryBase<RowData> {

    /**
     * Creates new instance.
     *
     * @param fieldName field name that this instance represents, matching {@link RowData} column
     *     name.
     * @param fieldGetter {@link RowData.FieldGetter} for data type matching {@link RowData} column
     *     type that this instance represents.
     */
    public RowDataSingleValueLookupSchemaEntry(String fieldName, FieldGetter fieldGetter) {
        super(fieldName, fieldGetter);
    }

    /**
     * Creates single element collection that contains {@link LookupArg} for single column from
     * given lookupKeyRow. The column is defined by 'fieldName' and 'fieldGetter' used for creating
     * {@link RowDataSingleValueLookupSchemaEntry} instance
     *
     * @param lookupKeyRow Element to get the values from for {@code LookupArg#getArgValue()}.
     * @return single element collection of {@link LookupArg}.
     */
    @Override
    public List<LookupArg> convertToLookupArg(RowData lookupKeyRow) {

        Object value = tryGetValue(lookupKeyRow);

        if (value == null) {
            return Collections.emptyList();
        }

        if (!(value instanceof BinaryStringData)) {
            log.debug("Unsupported Key Type {}. Trying simple toString().", value.getClass());
        }

        return Collections.singletonList(new LookupArg(getFieldName(), value.toString()));
    }

    private Object tryGetValue(RowData lookupKeyRow) {
        try {
            return fieldGetter.getFieldOrNull(lookupKeyRow);
        } catch (ClassCastException e) {
            throw new RuntimeException(
                    "Class cast exception on field getter for field " + getFieldName(), e);
        }
    }

    @lombok.Generated
    @Override
    public String toString() {
        return "RowDataSingleValueLookupSchemaEntry{"
                + "fieldName='"
                + fieldName
                + '\''
                + ", fieldGetter="
                + fieldGetter
                + '}';
    }
}
