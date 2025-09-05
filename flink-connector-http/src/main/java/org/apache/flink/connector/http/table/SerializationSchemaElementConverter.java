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

package org.apache.flink.connector.http.table;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.connector.sink2.Sink.InitContext;
import org.apache.flink.api.connector.sink2.SinkWriter.Context;
import org.apache.flink.connector.http.SchemaLifecycleAwareElementConverter;
import org.apache.flink.connector.http.sink.HttpSinkRequestEntry;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.FlinkRuntimeException;

/** Serialization Schema Element Converter. */
public class SerializationSchemaElementConverter
        implements SchemaLifecycleAwareElementConverter<RowData, HttpSinkRequestEntry> {

    private final String insertMethod;

    private final SerializationSchema<RowData> serializationSchema;

    private boolean schemaOpened = false;

    public SerializationSchemaElementConverter(
            String insertMethod, SerializationSchema<RowData> serializationSchema) {

        this.insertMethod = insertMethod;
        this.serializationSchema = serializationSchema;
    }

    @Override
    public void open(InitContext context) {
        if (!schemaOpened) {
            try {
                serializationSchema.open(context.asSerializationSchemaInitializationContext());
                schemaOpened = true;
            } catch (Exception e) {
                throw new FlinkRuntimeException("Failed to initialize serialization schema.", e);
            }
        }
    }

    @Override
    public HttpSinkRequestEntry apply(RowData rowData, Context context) {
        return new HttpSinkRequestEntry(insertMethod, serializationSchema.serialize(rowData));
    }
}
