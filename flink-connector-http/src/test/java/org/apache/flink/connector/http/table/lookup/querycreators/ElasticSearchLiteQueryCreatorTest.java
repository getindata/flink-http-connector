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

package org.apache.flink.connector.http.table.lookup.querycreators;

import org.apache.flink.connector.http.table.lookup.LookupRow;
import org.apache.flink.connector.http.table.lookup.RowDataSingleValueLookupSchemaEntry;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.DataType;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.List;

import static org.apache.flink.connector.http.table.lookup.HttpLookupTableSourceFactory.row;
import static org.assertj.core.api.Assertions.assertThat;

/** ElasticSearchLiteQueryCreatorTest. */
public class ElasticSearchLiteQueryCreatorTest {

    @Test
    public void testWithEmptyLookupResult() {

        // GIVEN
        LookupRow lookupRow = new LookupRow();
        lookupRow.setLookupPhysicalRowDataType(DataTypes.STRING());

        GenericRowData lookupDataRow = GenericRowData.of(StringData.fromString("val1"));

        // WHEN
        var queryCreator = new ElasticSearchLiteQueryCreator(lookupRow);
        var createdQuery = queryCreator.createLookupQuery(lookupDataRow);

        // THEN
        assertThat(createdQuery.getLookupQuery()).isEqualTo("");
        assertThat(createdQuery.getBodyBasedUrlQueryParameters()).isEmpty();
    }

    @Test
    public void testQueryCreationForSingleQueryStringParam() {

        // GIVEN
        LookupRow lookupRow =
                new LookupRow()
                        .addLookupEntry(
                                new RowDataSingleValueLookupSchemaEntry(
                                        "key1",
                                        RowData.createFieldGetter(
                                                DataTypes.STRING().getLogicalType(), 0)));
        lookupRow.setLookupPhysicalRowDataType(DataTypes.STRING());

        GenericRowData lookupDataRow = GenericRowData.of(StringData.fromString("val1"));

        // WHEN
        var queryCreator = new ElasticSearchLiteQueryCreator(lookupRow);
        var createdQuery = queryCreator.createLookupQuery(lookupDataRow);

        // THEN
        assertThat(createdQuery.getLookupQuery()).isEqualTo("q=key1:%22val1%22");
        assertThat(createdQuery.getBodyBasedUrlQueryParameters()).isEmpty();
    }

    @Test
    public void testQueryCreationForSingleQueryIntParam() {

        // GIVEN
        BigDecimal decimalValue = BigDecimal.valueOf(10);
        DataType decimalValueType =
                DataTypes.DECIMAL(decimalValue.precision(), decimalValue.scale());

        LookupRow lookupRow =
                new LookupRow()
                        .addLookupEntry(
                                new RowDataSingleValueLookupSchemaEntry(
                                        "key1",
                                        RowData.createFieldGetter(
                                                decimalValueType.getLogicalType(), 0)));
        lookupRow.setLookupPhysicalRowDataType(decimalValueType);

        GenericRowData lookupDataRow =
                GenericRowData.of(
                        DecimalData.fromBigDecimal(
                                decimalValue, decimalValue.precision(), decimalValue.scale()));

        // WHEN
        var queryCreator = new ElasticSearchLiteQueryCreator(lookupRow);
        var createdQuery = queryCreator.createLookupQuery(lookupDataRow);

        // THEN
        assertThat(createdQuery.getLookupQuery()).isEqualTo("q=key1:%2210%22");
        assertThat(createdQuery.getBodyBasedUrlQueryParameters()).isEmpty();
    }

    @Test
    public void testGenericGetQueryCreationForMultipleQueryParam() {

        // GIVEN
        LookupRow lookupRow =
                new LookupRow()
                        .addLookupEntry(
                                new RowDataSingleValueLookupSchemaEntry(
                                        "key1",
                                        RowData.createFieldGetter(
                                                DataTypes.STRING().getLogicalType(), 0)))
                        .addLookupEntry(
                                new RowDataSingleValueLookupSchemaEntry(
                                        "key2",
                                        RowData.createFieldGetter(
                                                DataTypes.STRING().getLogicalType(), 1)))
                        .addLookupEntry(
                                new RowDataSingleValueLookupSchemaEntry(
                                        "key3",
                                        RowData.createFieldGetter(
                                                DataTypes.STRING().getLogicalType(), 2)));

        lookupRow.setLookupPhysicalRowDataType(
                row(
                        List.of(
                                DataTypes.FIELD("key1", DataTypes.STRING()),
                                DataTypes.FIELD("key2", DataTypes.STRING()),
                                DataTypes.FIELD("key3", DataTypes.STRING()))));

        GenericRowData lookupDataRow =
                GenericRowData.of(
                        StringData.fromString("val1"),
                        StringData.fromString("val2"),
                        StringData.fromString("3"));

        // WHEN
        var queryCreator = new ElasticSearchLiteQueryCreator(lookupRow);
        var createdQuery = queryCreator.createLookupQuery(lookupDataRow);

        // THEN
        assertThat(createdQuery.getLookupQuery())
                .isEqualTo("q=key1:%22val1%22%20AND%20key2:%22val2%22%20AND%20key3:%223%22");
        assertThat(createdQuery.getBodyBasedUrlQueryParameters()).isEmpty();
    }
}
