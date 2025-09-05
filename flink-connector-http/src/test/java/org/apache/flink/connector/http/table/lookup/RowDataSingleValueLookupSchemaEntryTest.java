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

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link RowDataSingleValueLookupSchemaEntry}. */
class RowDataSingleValueLookupSchemaEntryTest {

    // TODO Convert this to parametrized test and check all Flink types (Int, String etc).
    @Test
    public void shouldConvertFromSingleValue() {

        RowDataSingleValueLookupSchemaEntry entry =
                new RowDataSingleValueLookupSchemaEntry(
                        "col1", RowData.createFieldGetter(DataTypes.BOOLEAN().getLogicalType(), 0));

        List<LookupArg> lookupArgs = entry.convertToLookupArg(GenericRowData.of(true));

        assertThat(lookupArgs).containsExactly(new LookupArg("col1", "true"));
    }
}
