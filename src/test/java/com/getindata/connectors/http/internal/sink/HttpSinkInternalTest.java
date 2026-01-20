package com.getindata.connectors.http.internal.sink;

import com.getindata.connectors.http.HttpPostRequestCallback;
import com.getindata.connectors.http.SchemaLifecycleAwareElementConverter;
import com.getindata.connectors.http.internal.HeaderPreprocessor;
import com.getindata.connectors.http.internal.SinkHttpClient;
import com.getindata.connectors.http.internal.SinkHttpClientBuilder;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.connector.base.sink.writer.ElementConverter;
import org.apache.flink.metrics.groups.OperatorIOMetricGroup;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Properties;

import static org.mockito.Mockito.*;

public class HttpSinkInternalTest {

    @Test
    void testCreateWriterInitializesLifecycleAwareConverter() throws Exception {
        SchemaLifecycleAwareElementConverter<?, ?> mockConverter = mock(SchemaLifecycleAwareElementConverter.class);
        Sink.InitContext mockContext = mock(Sink.InitContext.class);
        SinkWriterMetricGroup mockMetricGroup = mock(SinkWriterMetricGroup.class);
        when(mockContext.metricGroup()).thenReturn(mockMetricGroup);
        OperatorIOMetricGroup mockIOMetricGroup = mock(OperatorIOMetricGroup.class);
        when(mockMetricGroup.getIOMetricGroup()).thenReturn(mockIOMetricGroup);

        HttpSinkInternal<Object> httpSink = createTestSink((ElementConverter<Object, HttpSinkRequestEntry>) mockConverter);
        httpSink.createWriter(mockContext);
        // com.getindata.connectors.http.internal.sink.HttpSinkInternal.initElementConverterOfSchema
        // org.apache.flink.connector.base.sink.writer.AsyncSinkWriter
        verify(mockConverter, times(2)).open(mockContext);
    }

    @Test
    void testRestoreWriterInitializesLifecycleAwareConverter() throws Exception {

        SchemaLifecycleAwareElementConverter<?, ?> mockConverter = mock(SchemaLifecycleAwareElementConverter.class);
        Sink.InitContext mockContext = mock(Sink.InitContext.class);
        SinkWriterMetricGroup mockMetricGroup = mock(SinkWriterMetricGroup.class);
        when(mockContext.metricGroup()).thenReturn(mockMetricGroup);
        when(mockMetricGroup.getIOMetricGroup()).thenReturn(mock(OperatorIOMetricGroup.class));
        HttpSinkInternal<Object> httpSink = createTestSink((ElementConverter<Object, HttpSinkRequestEntry>) mockConverter);

        httpSink.restoreWriter(mockContext, Collections.emptyList());
        // com.getindata.connectors.http.internal.sink.HttpSinkInternal.initElementConverterOfSchema
        // org.apache.flink.connector.base.sink.writer.AsyncSinkWriter
        verify(mockConverter, times(2)).open(mockContext);
    }


    private HttpSinkInternal<Object> createTestSink(ElementConverter<Object, HttpSinkRequestEntry> converter) {
        SinkHttpClientBuilder mockClientBuilder = mock(SinkHttpClientBuilder.class);
        when(mockClientBuilder.build(any(), any(), any(), any()))
                .thenReturn(mock(SinkHttpClient.class));

        return new HttpSinkInternal<>(
                converter,
                10, 1, 20, 1024L, 1000L, 1024L,
                "http://test-endpoint.com",
                mock(HttpPostRequestCallback.class),
                mock(HeaderPreprocessor.class),
                mockClientBuilder,
                new Properties()
        );
    }

}
