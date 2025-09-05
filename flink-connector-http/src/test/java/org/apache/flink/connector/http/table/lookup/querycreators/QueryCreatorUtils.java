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

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.http.table.lookup.LookupRow;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.SerializationFormatFactory;

import java.util.Collections;

/** Query Creator utils. */
public class QueryCreatorUtils {
    public static DynamicTableFactory.Context getTableContext(
            Configuration config, ResolvedSchema resolvedSchema) {

        return new FactoryUtil.DefaultDynamicTableContext(
                ObjectIdentifier.of("default", "default", "test"),
                new ResolvedCatalogTable(
                        CatalogTable.of(
                                Schema.newBuilder().fromResolvedSchema(resolvedSchema).build(),
                                null,
                                Collections.emptyList(),
                                Collections.emptyMap()),
                        resolvedSchema),
                Collections.emptyMap(),
                config,
                Thread.currentThread().getContextClassLoader(),
                false);
    }

    public static SerializationSchema<RowData> getRowDataSerializationSchema(
            LookupRow lookupRow,
            DynamicTableFactory.Context dynamicTableFactoryContext,
            String formatIdentifier,
            QueryFormatAwareConfiguration queryFormatAwareConfiguration) {
        SerializationFormatFactory jsonFormatFactory =
                FactoryUtil.discoverFactory(
                        dynamicTableFactoryContext.getClassLoader(),
                        SerializationFormatFactory.class,
                        formatIdentifier);

        EncodingFormat<SerializationSchema<RowData>> encoder =
                jsonFormatFactory.createEncodingFormat(
                        dynamicTableFactoryContext, queryFormatAwareConfiguration);

        final SerializationSchema<RowData> serializationSchema =
                encoder.createRuntimeEncoder(null, lookupRow.getLookupPhysicalRowDataType());
        return serializationSchema;
    }
}
