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
import org.apache.flink.connector.http.LookupQueryCreator;
import org.apache.flink.connector.http.table.lookup.LookupQueryInfo;
import org.apache.flink.connector.http.utils.SerializationSchemaUtils;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.FlinkRuntimeException;

import java.nio.charset.StandardCharsets;

/** A {@link LookupQueryCreator} that builds Json based body for REST requests, i.e. adds */
public class GenericJsonQueryCreator implements LookupQueryCreator {

    /** The {@link SerializationSchema} to serialize {@link RowData} object. */
    private final SerializationSchema<RowData> jsonSerialization;

    private boolean schemaOpened = false;

    public GenericJsonQueryCreator(SerializationSchema<RowData> jsonSerialization) {

        this.jsonSerialization = jsonSerialization;
    }

    /**
     * Creates a Jason string from given {@link RowData}.
     *
     * @param lookupDataRow {@link RowData} to serialize into Json string.
     * @return Json string created from lookupDataRow argument.
     */
    @Override
    public LookupQueryInfo createLookupQuery(RowData lookupDataRow) {
        checkOpened();
        String lookupQuery =
                new String(jsonSerialization.serialize(lookupDataRow), StandardCharsets.UTF_8);

        return new LookupQueryInfo(lookupQuery);
    }

    private void checkOpened() {
        if (!schemaOpened) {
            try {
                jsonSerialization.open(
                        SerializationSchemaUtils.createSerializationInitContext(
                                GenericJsonQueryCreator.class));
            } catch (Exception e) {
                throw new FlinkRuntimeException(
                        "Failed to initialize serialization schema for GenericJsonQueryCreatorFactory.",
                        e);
            }
            schemaOpened = true;
        }
    }
}
