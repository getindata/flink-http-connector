package com.getindata.connectors.http.internal.sink;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.connector.sink2.Sink.InitContext;
import org.apache.flink.connector.base.sink.writer.BufferedRequestState;
import org.apache.flink.connector.base.sink.writer.ElementConverter;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.getindata.connectors.http.internal.SinkHttpClient;
import com.getindata.connectors.http.internal.SinkHttpClientResponse;

@Slf4j
@ExtendWith(MockitoExtension.class)
class HttpSinkWriterTest {

    private HttpSinkWriter<String> httpSinkWriter;

    @Mock
    private ElementConverter<String, HttpSinkRequestEntry> elementConverter;

    @Mock
    private InitContext context;

    @Mock
    private SinkHttpClient httpClient;

    @Mock
    private SinkWriterMetricGroup metricGroup;

    @Mock
    private Counter errorCounter;

    @BeforeEach
    public void setUp() {
        when(metricGroup.getNumRecordsSendErrorsCounter()).thenReturn(errorCounter);
        when(context.metricGroup()).thenReturn(metricGroup);

        Collection<BufferedRequestState<HttpSinkRequestEntry>> stateBuffer = new ArrayList<>();

        this.httpSinkWriter = new HttpSinkWriter<>(
            elementConverter,
            context,
            10,
            10,
            100,
            10,
            10,
            10,
            "http://localhost/client",
            httpClient,
            stateBuffer,
            new Properties());
    }

    @Test
    public void testErrorMetric() throws InterruptedException {

        CompletableFuture<SinkHttpClientResponse> future = new CompletableFuture<>();
        future.completeExceptionally(new Exception("Test Exception"));

        when(httpClient.putRequests(anyList(), anyString())).thenReturn(future);

        HttpSinkRequestEntry request = new HttpSinkRequestEntry("PUT", "hello".getBytes());
        Consumer<List<HttpSinkRequestEntry>> requestResult =
            httpSinkRequestEntries -> log.info(String.valueOf(httpSinkRequestEntries));

        List<HttpSinkRequestEntry> requestEntries = Collections.singletonList(request);
        this.httpSinkWriter.submitRequestEntries(requestEntries, requestResult);

        // would be good to use Countdown Latch instead sleep...
        Thread.sleep(2000);
        verify(errorCounter).inc(requestEntries.size());
    }
}
