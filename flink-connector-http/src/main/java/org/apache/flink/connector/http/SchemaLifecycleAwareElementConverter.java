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

package org.apache.flink.connector.http;

import org.apache.flink.api.common.serialization.SerializationSchema.InitializationContext;
import org.apache.flink.api.connector.sink2.Sink.InitContext;
import org.apache.flink.connector.base.sink.writer.ElementConverter;

/**
 * An enhancement for Flink's {@link ElementConverter} that expose {@link #open(InitContext)} method
 * that will be called by HTTP connect code to ensure that element converter is initialized
 * properly. This is required for cases when Flink's SerializationSchema and DeserializationSchema
 * objects like JsonRowDataSerializationSchema are used.
 *
 * <p>This interface specifies the mapping between elements of a stream to request entries that can
 * be sent to the destination. The mapping is provided by the end-user of a sink, not the sink
 * creator.
 *
 * <p>The request entries contain all relevant information required to create and sent the actual
 * request. Eg, for Kinesis Data Streams, the request entry includes the payload and the partition
 * key.
 */
public interface SchemaLifecycleAwareElementConverter<InputT, RequestEntryT>
        extends ElementConverter<InputT, RequestEntryT> {

    /**
     * Initialization element converter for the schema.
     *
     * <p>The provided {@link InitializationContext} can be used to access additional features such
     * as e.g. registering user metrics.
     *
     * @param context Contextual information that can be used during initialization.
     */
    void open(InitContext context);
}
