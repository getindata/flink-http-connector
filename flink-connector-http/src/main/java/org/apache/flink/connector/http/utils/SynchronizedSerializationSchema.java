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

package org.apache.flink.connector.http.utils;

import org.apache.flink.api.common.serialization.SerializationSchema;

/**
 * Decorator which ensures that underlying SerializationSchema is called in thread-safe way.
 *
 * @param <T> type
 */
public class SynchronizedSerializationSchema<T> implements SerializationSchema<T> {

    private final SerializationSchema<T> delegate;

    public SynchronizedSerializationSchema(SerializationSchema<T> delegate) {
        this.delegate = delegate;
    }

    @Override
    public void open(InitializationContext context) throws Exception {
        doOpen(context);
    }

    private synchronized void doOpen(InitializationContext context) throws Exception {
        this.delegate.open(context);
    }

    @Override
    public byte[] serialize(T element) {
        return syncSerialize(element);
    }

    private synchronized byte[] syncSerialize(T element) {
        return delegate.serialize(element);
    }
}
