package com.getindata.connectors.http.internal.utils;

import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.util.SimpleUserCodeClassLoader;
import org.apache.flink.util.UserCodeClassLoader;

public final class SerializationSchemaUtils {

    private SerializationSchemaUtils() {

    }

    public static <T> org.apache.flink.api.common.serialization.SerializationSchema
            .InitializationContext createSerializationInitContext(Class<T> classForClassLoader) {

        return new org.apache.flink.api.common.serialization.SerializationSchema
                .InitializationContext() {

            @Override
            public MetricGroup getMetricGroup() {
                return new UnregisteredMetricsGroup();
            }

            @Override
            public UserCodeClassLoader getUserCodeClassLoader() {
                return SimpleUserCodeClassLoader.create(classForClassLoader.getClassLoader());
            }
        };
    }

    public static <T> org.apache.flink.api.common.serialization.DeserializationSchema
        .InitializationContext createDeserializationInitContext(Class<T> classForClassLoader) {

        return new org.apache.flink.api.common.serialization.DeserializationSchema
            .InitializationContext() {

            @Override
            public MetricGroup getMetricGroup() {
                return new UnregisteredMetricsGroup();
            }

            @Override
            public UserCodeClassLoader getUserCodeClassLoader() {
                return SimpleUserCodeClassLoader.create(classForClassLoader.getClassLoader());
            }
        };
    }

}
