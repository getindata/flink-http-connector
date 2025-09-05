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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.http.table.lookup.LookupRow;
import org.apache.flink.connector.http.table.lookup.RowDataSingleValueLookupSchemaEntry;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.FactoryUtil;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;

import static org.apache.flink.connector.http.table.lookup.HttpLookupTableSourceFactory.row;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link GenericJsonQueryCreatorFactory }. */
class GenericJsonQueryCreatorFactoryTest {

    private Configuration config;
    private LookupRow lookupRow;

    private DynamicTableFactory.Context tableContext;

    @BeforeEach
    public void setUp() {
        this.config = new Configuration();
        this.lookupRow = new LookupRow();
        lookupRow =
                new LookupRow()
                        .addLookupEntry(
                                new RowDataSingleValueLookupSchemaEntry(
                                        "key1",
                                        RowData.createFieldGetter(
                                                DataTypes.STRING().getLogicalType(), 0)));

        lookupRow.setLookupPhysicalRowDataType(
                row(List.of(DataTypes.FIELD("key1", DataTypes.STRING()))));

        CustomFormatFactory.requiredOptionsWereUsed = false;

        ResolvedSchema resolvedSchema =
                ResolvedSchema.of(Column.physical("key1", DataTypes.STRING()));

        this.tableContext =
                new FactoryUtil.DefaultDynamicTableContext(
                        ObjectIdentifier.of("default", "default", "test"),
                        new ResolvedCatalogTable(
                                CatalogTable.of(
                                        Schema.newBuilder()
                                                .fromResolvedSchema(resolvedSchema)
                                                .build(),
                                        null,
                                        Collections.emptyList(),
                                        Collections.emptyMap()),
                                resolvedSchema),
                        Collections.emptyMap(),
                        config,
                        Thread.currentThread().getContextClassLoader(),
                        false);
    }

    @Test
    public void shouldPassPropertiesToQueryCreatorFormat() {
        assertThat(CustomFormatFactory.requiredOptionsWereUsed)
                .withFailMessage(
                        "CustomFormatFactory was not cleared, "
                                + "make sure `CustomFormatFactory.requiredOptionsWereUsed = false` "
                                + "was called before this test execution.")
                .isFalse();

        this.config.setString("lookup-request.format", CustomFormatFactory.IDENTIFIER);
        this.config.setString(
                String.format(
                        "lookup-request.format.%s.%s",
                        CustomFormatFactory.IDENTIFIER, CustomFormatFactory.REQUIRED_OPTION),
                "optionValue");

        new GenericJsonQueryCreatorFactory()
                .createLookupQueryCreator(config, lookupRow, tableContext);

        assertThat(CustomFormatFactory.requiredOptionsWereUsed).isTrue();
    }
}
