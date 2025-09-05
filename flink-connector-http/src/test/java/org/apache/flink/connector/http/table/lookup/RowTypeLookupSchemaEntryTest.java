/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.http.table.lookup;

import org.apache.flink.connector.http.LookupArg;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link RowTypeLookupSchemaEntry}. */
class RowTypeLookupSchemaEntryTest {

    @Test
    public void testEmptyRow() {
        // GIVEN
        RowTypeLookupSchemaEntry lookupSchemaEntry =
                new RowTypeLookupSchemaEntry(
                                "aRow",
                                RowData.createFieldGetter(
                                        DataTypes.ROW(DataTypes.FIELD("col1", DataTypes.STRING()))
                                                .getLogicalType(),
                                        0))
                        .addLookupEntry(
                                new RowDataSingleValueLookupSchemaEntry(
                                        "col1",
                                        RowData.createFieldGetter(
                                                DataTypes.STRING().getLogicalType(), 0)));

        GenericRowData rowData = new GenericRowData(1);
        rowData.setField(0, null);

        // WHEN
        List<LookupArg> lookupArgs = lookupSchemaEntry.convertToLookupArg(rowData);

        // THEN
        assertThat(lookupArgs).isEmpty();
    }

    @Test
    public void testRowWithMultipleSingleValues() {

        // GIVEN
        RowTypeLookupSchemaEntry lookupSchemaEntry =
                new RowTypeLookupSchemaEntry(
                                "aRow",
                                RowData.createFieldGetter(
                                        DataTypes.ROW(
                                                        DataTypes.FIELD("col1", DataTypes.STRING()),
                                                        DataTypes.FIELD("col2", DataTypes.STRING()),
                                                        DataTypes.FIELD("col3", DataTypes.STRING()))
                                                .getLogicalType(),
                                        0))
                        .addLookupEntry(
                                new RowDataSingleValueLookupSchemaEntry(
                                        "col1",
                                        RowData.createFieldGetter(
                                                DataTypes.STRING().getLogicalType(), 0)))
                        .addLookupEntry(
                                new RowDataSingleValueLookupSchemaEntry(
                                        "col2",
                                        RowData.createFieldGetter(
                                                DataTypes.STRING().getLogicalType(), 1)))
                        .addLookupEntry(
                                new RowDataSingleValueLookupSchemaEntry(
                                        "col3",
                                        RowData.createFieldGetter(
                                                DataTypes.STRING().getLogicalType(), 2)));

        GenericRowData rowData =
                GenericRowData.of(
                        GenericRowData.of(
                                StringData.fromString("val1"),
                                StringData.fromString("val2"),
                                StringData.fromString("val3")));

        // WHEN
        List<LookupArg> lookupArgs = lookupSchemaEntry.convertToLookupArg(rowData);

        // THEN
        assertThat(lookupArgs)
                .containsExactly(
                        new LookupArg("col1", "val1"),
                        new LookupArg("col2", "val2"),
                        new LookupArg("col3", "val3"));
    }

    @Test
    public void testRowWithNestedRowValues() {

        // GIVEN
        RowTypeLookupSchemaEntry nestedRowLookupSchemaEntry =
                new RowTypeLookupSchemaEntry(
                                "aRow",
                                RowData.createFieldGetter(
                                        DataTypes.FIELD(
                                                        "nestedRow",
                                                        DataTypes.ROW(
                                                                DataTypes.FIELD(
                                                                        "col1", DataTypes.STRING()),
                                                                DataTypes.FIELD(
                                                                        "col2",
                                                                        DataTypes.STRING())))
                                                .getDataType()
                                                .getLogicalType(),
                                        0))
                        .addLookupEntry(
                                new RowDataSingleValueLookupSchemaEntry(
                                        "col1",
                                        RowData.createFieldGetter(
                                                DataTypes.STRING().getLogicalType(), 0)))
                        .addLookupEntry(
                                new RowDataSingleValueLookupSchemaEntry(
                                        "col2",
                                        RowData.createFieldGetter(
                                                DataTypes.STRING().getLogicalType(), 1)));

        RowTypeLookupSchemaEntry rootSchemaEntry =
                new RowTypeLookupSchemaEntry(
                                "aRow",
                                RowData.createFieldGetter(
                                        DataTypes.ROW(
                                                        DataTypes.ROW(
                                                                DataTypes.FIELD(
                                                                        "nestedRow",
                                                                        DataTypes.ROW(
                                                                                DataTypes.FIELD(
                                                                                        "col1",
                                                                                        DataTypes
                                                                                                .STRING()),
                                                                                DataTypes.FIELD(
                                                                                        "col2",
                                                                                        DataTypes
                                                                                                .STRING())))),
                                                        DataTypes.FIELD("col3", DataTypes.STRING())
                                                                .getDataType())
                                                .getLogicalType(),
                                        0))
                        .addLookupEntry(nestedRowLookupSchemaEntry);

        GenericRowData rowData =
                GenericRowData.of(
                        GenericRowData.of(
                                GenericRowData.of(
                                        StringData.fromString("val1"),
                                        StringData.fromString("val2"))));

        // WHEN
        List<LookupArg> lookupArgs = rootSchemaEntry.convertToLookupArg(rowData);

        // THEN
        assertThat(lookupArgs)
                .containsExactly(new LookupArg("col1", "val1"), new LookupArg("col2", "val2"));
    }
}
