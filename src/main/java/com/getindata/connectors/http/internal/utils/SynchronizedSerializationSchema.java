package com.getindata.connectors.http.internal.utils;

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
