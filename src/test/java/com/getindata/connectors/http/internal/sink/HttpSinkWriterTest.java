package com.getindata.connectors.http.internal.sink;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.connector.sink2.Sink.InitContext;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.base.sink.writer.BufferedRequestState;
import org.apache.flink.connector.base.sink.writer.ElementConverter;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.groups.OperatorIOMetricGroup;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.getindata.connectors.http.internal.SinkHttpClient;
import com.getindata.connectors.http.internal.SinkHttpClientResponse;
import com.getindata.connectors.http.internal.SinkHttpClientResponse.ResponseItem;
import com.getindata.connectors.http.internal.config.ResponseItemStatus;

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

    // To work with Flink 1.15 and Flink 1.16
    @Mock(lenient = true)
    private SinkWriterMetricGroup metricGroup;

    @Mock
    private OperatorIOMetricGroup operatorIOMetricGroup;

    @Mock
    private Counter errorCounter;

    @BeforeEach
    public void setUp() {
        when(metricGroup.getNumRecordsSendErrorsCounter()).thenReturn(errorCounter);
        when(metricGroup.getIOMetricGroup()).thenReturn(operatorIOMetricGroup);
        when(context.metricGroup()).thenReturn(metricGroup);
    }

    private void createHttpSinkWriter(DeliveryGuarantee deliveryGuarantee) {
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
                deliveryGuarantee,
                "http://localhost/client",
                httpClient,
                stateBuffer,
                new Properties());
    }

    @Test
    public void testErrorMetricWhenAllRequestsFailed() throws InterruptedException {
        createHttpSinkWriter(DeliveryGuarantee.NONE);

        CompletableFuture<SinkHttpClientResponse> future = new CompletableFuture<>();
        future.completeExceptionally(new Exception("Test Exception"));

        when(httpClient.putRequests(anyList(), anyString())).thenReturn(future);

        HttpSinkRequestEntry request1 = new HttpSinkRequestEntry("PUT", "hello".getBytes());
        HttpSinkRequestEntry request2 = new HttpSinkRequestEntry("PUT", "world".getBytes());
        Consumer<List<HttpSinkRequestEntry>> requestResult =
            httpSinkRequestEntries -> log.info(String.valueOf(httpSinkRequestEntries));

        List<HttpSinkRequestEntry> requestEntries = Arrays.asList(request1, request2);
        this.httpSinkWriter.submitRequestEntries(requestEntries, requestResult);

        // would be good to use Countdown Latch instead sleep...
        Thread.sleep(2000);
        verify(errorCounter).inc(2);
    }

    @Test
    public void testErrorMetricWhenFailureRequestsOccur() throws InterruptedException {
        createHttpSinkWriter(DeliveryGuarantee.NONE);

        CompletableFuture<SinkHttpClientResponse> future = new CompletableFuture<>();
        future.complete(new SinkHttpClientResponse(
            Arrays.asList(
                new ResponseItem(null, ResponseItemStatus.SUCCESS),
                new ResponseItem(null, ResponseItemStatus.IGNORE),
                new ResponseItem(null, ResponseItemStatus.TEMPORAL),
                new ResponseItem(null, ResponseItemStatus.FAILURE))
        ));

        when(httpClient.putRequests(anyList(), anyString())).thenReturn(future);

        HttpSinkRequestEntry request1 = new HttpSinkRequestEntry("PUT", "hello".getBytes());
        HttpSinkRequestEntry request2 = new HttpSinkRequestEntry("PUT", "world".getBytes());
        HttpSinkRequestEntry request3 = new HttpSinkRequestEntry("PUT", "lorem".getBytes());
        HttpSinkRequestEntry request4 = new HttpSinkRequestEntry("PUT", "ipsum".getBytes());
        Consumer<List<HttpSinkRequestEntry>> requestResult =
            httpSinkRequestEntries -> log.info(String.valueOf(httpSinkRequestEntries));

        List<HttpSinkRequestEntry> requestEntries = Arrays.asList(request1, request2, request3, request4);
        this.httpSinkWriter.submitRequestEntries(requestEntries, requestResult);

        // would be good to use Countdown Latch instead sleep...
        Thread.sleep(2000);
        verify(errorCounter).inc(1);
    }

    @Test
    public void testRetryWhenAllRequestsFailed() throws InterruptedException {
        createHttpSinkWriter(DeliveryGuarantee.AT_LEAST_ONCE);

        CompletableFuture<SinkHttpClientResponse> future = new CompletableFuture<>();
        future.completeExceptionally(new Exception("Test Exception"));

        when(httpClient.putRequests(anyList(), anyString())).thenReturn(future);

        final List<HttpSinkRequestEntry> entriesToRetry = new ArrayList<>();

        HttpSinkRequestEntry request1 = new HttpSinkRequestEntry("PUT", "hello".getBytes());
        HttpSinkRequestEntry request2 = new HttpSinkRequestEntry("PUT", "world".getBytes());
        Consumer<List<HttpSinkRequestEntry>> requestResult = entriesToRetry::addAll;

        List<HttpSinkRequestEntry> requestEntries = Arrays.asList(request1, request2);
        this.httpSinkWriter.submitRequestEntries(requestEntries, requestResult);

        // would be good to use Countdown Latch instead sleep...
        Thread.sleep(2000);
        verify(errorCounter).inc(2);
        assertEquals(2, entriesToRetry.size());
    }


    @Test
    public void testRetryWhenAPartOfRequestsFailed() throws InterruptedException {
        createHttpSinkWriter(DeliveryGuarantee.AT_LEAST_ONCE);

        CompletableFuture<SinkHttpClientResponse> future = new CompletableFuture<>();
        future.complete(new SinkHttpClientResponse(
            Arrays.asList(
                new ResponseItem(null, ResponseItemStatus.TEMPORAL),
                new ResponseItem(null, ResponseItemStatus.SUCCESS))
        ));

        when(httpClient.putRequests(anyList(), anyString())).thenReturn(future);

        final List<HttpSinkRequestEntry> entriesToRetry = new ArrayList<>();

        HttpSinkRequestEntry request1 = new HttpSinkRequestEntry("PUT", "hello".getBytes());
        HttpSinkRequestEntry request2 = new HttpSinkRequestEntry("PUT", "world".getBytes());
        Consumer<List<HttpSinkRequestEntry>> requestResult = entriesToRetry::addAll;

        List<HttpSinkRequestEntry> requestEntries = Arrays.asList(request1, request2);
        this.httpSinkWriter.submitRequestEntries(requestEntries, requestResult);

        // would be good to use Countdown Latch instead sleep...
        Thread.sleep(2000);
        verify(errorCounter).inc(1);
        assertEquals(1, entriesToRetry.size());
    }

    @Test
    public void testTemporalRequestsWithNoneGuaranteeDoNotRetry() throws InterruptedException {
        createHttpSinkWriter(DeliveryGuarantee.NONE);

        CompletableFuture<SinkHttpClientResponse> future = new CompletableFuture<>();
        future.complete(new SinkHttpClientResponse(
            Arrays.asList(
                new ResponseItem(null, ResponseItemStatus.TEMPORAL),
                new ResponseItem(null, ResponseItemStatus.SUCCESS))
        ));

        when(httpClient.putRequests(anyList(), anyString())).thenReturn(future);

        final List<HttpSinkRequestEntry> entriesToRetry = new ArrayList<>();

        HttpSinkRequestEntry request1 = new HttpSinkRequestEntry("PUT", "hello".getBytes());
        HttpSinkRequestEntry request2 = new HttpSinkRequestEntry("PUT", "world".getBytes());
        Consumer<List<HttpSinkRequestEntry>> requestResult = entriesToRetry::addAll;

        List<HttpSinkRequestEntry> requestEntries = Arrays.asList(request1, request2);
        this.httpSinkWriter.submitRequestEntries(requestEntries, requestResult);

        // would be good to use Countdown Latch instead sleep...
        Thread.sleep(2000);
        verify(errorCounter).inc(1);
        assertEquals(0, entriesToRetry.size());
    }

    @Test
    public void testAllSuccessfulRequests() throws InterruptedException {
        createHttpSinkWriter(DeliveryGuarantee.NONE);

        CompletableFuture<SinkHttpClientResponse> future = new CompletableFuture<>();
        future.complete(new SinkHttpClientResponse(
            Arrays.asList(
                new ResponseItem(null, ResponseItemStatus.SUCCESS),
                new ResponseItem(null, ResponseItemStatus.SUCCESS))
        ));

        when(httpClient.putRequests(anyList(), anyString())).thenReturn(future);

        final List<HttpSinkRequestEntry> entriesToRetry = new ArrayList<>();

        HttpSinkRequestEntry request1 = new HttpSinkRequestEntry("PUT", "hello".getBytes());
        HttpSinkRequestEntry request2 = new HttpSinkRequestEntry("PUT", "world".getBytes());
        Consumer<List<HttpSinkRequestEntry>> requestResult = entriesToRetry::addAll;

        List<HttpSinkRequestEntry> requestEntries = Arrays.asList(request1, request2);
        this.httpSinkWriter.submitRequestEntries(requestEntries, requestResult);

        // would be good to use Countdown Latch instead sleep...
        Thread.sleep(2000);
        assertEquals(0, entriesToRetry.size());
    }

    @Test
    public void testIgnoredRequests() throws InterruptedException {
        createHttpSinkWriter(DeliveryGuarantee.NONE);

        CompletableFuture<SinkHttpClientResponse> future = new CompletableFuture<>();
        future.complete(new SinkHttpClientResponse(
            Arrays.asList(
                new ResponseItem(null, ResponseItemStatus.IGNORE),
                new ResponseItem(null, ResponseItemStatus.SUCCESS))
        ));

        when(httpClient.putRequests(anyList(), anyString())).thenReturn(future);

        final List<HttpSinkRequestEntry> entriesToRetry = new ArrayList<>();

        HttpSinkRequestEntry request1 = new HttpSinkRequestEntry("PUT", "hello".getBytes());
        HttpSinkRequestEntry request2 = new HttpSinkRequestEntry("PUT", "world".getBytes());
        Consumer<List<HttpSinkRequestEntry>> requestResult = entriesToRetry::addAll;

        List<HttpSinkRequestEntry> requestEntries = Arrays.asList(request1, request2);
        this.httpSinkWriter.submitRequestEntries(requestEntries, requestResult);

        // would be good to use Countdown Latch instead sleep...
        Thread.sleep(2000);
        assertEquals(0, entriesToRetry.size());
    }

    @Test
    public void testFailureRequestsWithAtLeastOnceGuarantee() throws InterruptedException {
        createHttpSinkWriter(DeliveryGuarantee.AT_LEAST_ONCE);

        CompletableFuture<SinkHttpClientResponse> future = new CompletableFuture<>();
        future.complete(new SinkHttpClientResponse(
            Arrays.asList(
                new ResponseItem(null, ResponseItemStatus.FAILURE),
                new ResponseItem(null, ResponseItemStatus.SUCCESS))
        ));

        when(httpClient.putRequests(anyList(), anyString())).thenReturn(future);

        HttpSinkRequestEntry request1 = new HttpSinkRequestEntry("PUT", "hello".getBytes());
        HttpSinkRequestEntry request2 = new HttpSinkRequestEntry("PUT", "world".getBytes());
        Consumer<List<HttpSinkRequestEntry>> requestResult =
            httpSinkRequestEntries -> log.info(String.valueOf(httpSinkRequestEntries));

        List<HttpSinkRequestEntry> requestEntries = Arrays.asList(request1, request2);
        this.httpSinkWriter.submitRequestEntries(requestEntries, requestResult);

        // would be good to use Countdown Latch instead sleep...
        Thread.sleep(2000);
        verify(errorCounter).inc(1);
    }

    @Test
    public void testUnsupportedDeliveryGuaranteeThrowsException() throws InterruptedException {
        createHttpSinkWriter(DeliveryGuarantee.EXACTLY_ONCE);

        CompletableFuture<SinkHttpClientResponse> future = new CompletableFuture<>();
        future.completeExceptionally(new Exception("Test Exception"));

        when(httpClient.putRequests(anyList(), anyString())).thenReturn(future);

        HttpSinkRequestEntry request1 = new HttpSinkRequestEntry("PUT", "hello".getBytes());
        HttpSinkRequestEntry request2 = new HttpSinkRequestEntry("PUT", "world".getBytes());
        Consumer<List<HttpSinkRequestEntry>> requestResult =
            httpSinkRequestEntries -> log.info(String.valueOf(httpSinkRequestEntries));

        List<HttpSinkRequestEntry> requestEntries = Arrays.asList(request1, request2);
        this.httpSinkWriter.submitRequestEntries(requestEntries, requestResult);

        // would be good to use Countdown Latch instead sleep...
        Thread.sleep(2000);
        verify(errorCounter).inc(2);
    }
}
