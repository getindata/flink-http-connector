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
import org.apache.flink.connector.http.table.lookup.LookupQueryInfo;
import org.apache.flink.formats.json.JsonFormatFactory;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.factories.DynamicTableFactory.Context;
import org.apache.flink.table.types.DataType;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;

import static org.apache.flink.connector.http.table.lookup.HttpLookupTableSourceFactory.row;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link GenericJsonQueryCreator }. */
@ExtendWith(MockitoExtension.class)
class GenericJsonQueryCreatorTest {

    @Mock private Context dynamicTableFactoryContext;

    private GenericJsonQueryCreator jsonQueryCreator;

    @BeforeEach
    public void setUp() {

        DataType lookupPhysicalDataType =
                row(
                        List.of(
                                DataTypes.FIELD("id", DataTypes.INT()),
                                DataTypes.FIELD("uuid", DataTypes.STRING())));

        SerializationSchema<RowData> jsonSerializer =
                new JsonFormatFactory()
                        .createEncodingFormat(dynamicTableFactoryContext, new Configuration())
                        .createRuntimeEncoder(null, lookupPhysicalDataType);

        this.jsonQueryCreator = new GenericJsonQueryCreator(jsonSerializer);
    }

    @Test
    public void shouldSerializeToJson() {
        GenericRowData row = new GenericRowData(2);
        row.setField(0, 11);
        row.setField(1, StringData.fromString("myUuid"));

        LookupQueryInfo lookupQuery = this.jsonQueryCreator.createLookupQuery(row);
        assertThat(lookupQuery.getBodyBasedUrlQueryParameters().isEmpty());
        assertThat(lookupQuery.getLookupQuery()).isEqualTo("{\"id\":11,\"uuid\":\"myUuid\"}");
    }

    @Test
    public void shouldSerializeToJsonTwice() {
        GenericRowData row = new GenericRowData(2);
        row.setField(0, 11);
        row.setField(1, StringData.fromString("myUuid"));

        // Call createLookupQuery two times
        // to check that serialization schema is not opened Two times.
        this.jsonQueryCreator.createLookupQuery(row);
        LookupQueryInfo lookupQuery = this.jsonQueryCreator.createLookupQuery(row);
        assertThat(lookupQuery.getBodyBasedUrlQueryParameters().isEmpty());
        assertThat(lookupQuery.getLookupQuery()).isEqualTo("{\"id\":11,\"uuid\":\"myUuid\"}");
    }
}
