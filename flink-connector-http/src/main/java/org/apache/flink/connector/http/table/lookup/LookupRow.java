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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.connector.http.LookupArg;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.io.Serializable;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

/** lookup row. */
@ToString
public class LookupRow implements Serializable {

    private final List<LookupSchemaEntry<RowData>> lookupEntries;

    @Getter @Setter private DataType lookupPhysicalRowDataType;

    public LookupRow() {
        this.lookupEntries = new LinkedList<>();
    }

    /**
     * Creates a collection of {@link LookupArg} elements. Every column and its value from provided
     * {@link RowData} is converted to {@link LookupArg}.
     *
     * @param lookupDataRow A {@link RowData} to get the values from for {@code
     *     LookupArg#getArgValue()}.
     * @return Collection of {@link LookupArg} objects created from lookupDataRow.
     */
    public Collection<LookupArg> convertToLookupArgs(RowData lookupDataRow) {
        List<LookupArg> lookupArgs = new LinkedList<>();
        for (LookupSchemaEntry<RowData> lookupSchemaEntry : lookupEntries) {
            lookupArgs.addAll(lookupSchemaEntry.convertToLookupArg(lookupDataRow));
        }
        return lookupArgs;
    }

    public LookupRow addLookupEntry(LookupSchemaEntry<RowData> lookupSchemaEntry) {
        this.lookupEntries.add(lookupSchemaEntry);
        return this;
    }

    @VisibleForTesting
    List<LookupSchemaEntry<RowData>> getLookupEntries() {
        return new LinkedList<>(lookupEntries);
    }
}
