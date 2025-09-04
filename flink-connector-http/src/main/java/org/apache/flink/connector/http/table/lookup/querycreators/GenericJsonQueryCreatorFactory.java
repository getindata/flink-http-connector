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

package org.apache.flink.connector.http.table.lookup.querycreators;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.http.LookupQueryCreator;
import org.apache.flink.connector.http.LookupQueryCreatorFactory;
import org.apache.flink.connector.http.table.lookup.LookupRow;
import org.apache.flink.connector.http.utils.SynchronizedSerializationSchema;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.SerializationFormatFactory;

import java.util.Set;

import static org.apache.flink.connector.http.table.lookup.HttpLookupConnectorOptions.ASYNC_POLLING;
import static org.apache.flink.connector.http.table.lookup.HttpLookupConnectorOptions.LOOKUP_REQUEST_FORMAT;

/** Factory for creating {@link GenericJsonQueryCreatorFactory}. */
public class GenericJsonQueryCreatorFactory implements LookupQueryCreatorFactory {

    public static final String IDENTIFIER = "generic-json-query";

    @Override
    public LookupQueryCreator createLookupQueryCreator(
            ReadableConfig readableConfig,
            LookupRow lookupRow,
            DynamicTableFactory.Context dynamicTableFactoryContext) {

        String formatIdentifier = readableConfig.get(LOOKUP_REQUEST_FORMAT);
        SerializationFormatFactory jsonFormatFactory =
                FactoryUtil.discoverFactory(
                        dynamicTableFactoryContext.getClassLoader(),
                        SerializationFormatFactory.class,
                        formatIdentifier);
        QueryFormatAwareConfiguration queryFormatAwareConfiguration =
                new QueryFormatAwareConfiguration(
                        LOOKUP_REQUEST_FORMAT.key() + "." + formatIdentifier,
                        (Configuration) readableConfig);
        EncodingFormat<SerializationSchema<RowData>> encoder =
                jsonFormatFactory.createEncodingFormat(
                        dynamicTableFactoryContext, queryFormatAwareConfiguration);

        final SerializationSchema<RowData> serializationSchema;
        if (readableConfig.get(ASYNC_POLLING)) {
            serializationSchema =
                    new SynchronizedSerializationSchema<>(
                            encoder.createRuntimeEncoder(
                                    null, lookupRow.getLookupPhysicalRowDataType()));
        } else {
            serializationSchema =
                    encoder.createRuntimeEncoder(null, lookupRow.getLookupPhysicalRowDataType());
        }

        return new GenericJsonQueryCreator(serializationSchema);
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return Set.of();
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return Set.of();
    }
}
