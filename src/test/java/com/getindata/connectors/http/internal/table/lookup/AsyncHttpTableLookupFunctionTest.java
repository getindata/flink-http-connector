package com.getindata.connectors.http.internal.table.lookup;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.FunctionContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import static com.getindata.connectors.http.internal.table.lookup.TableSourceHelper.buildGenericRowData;

@Slf4j
@ExtendWith(MockitoExtension.class)
class AsyncHttpTableLookupFunctionTest {

    private final int[] rowKeys = {1, 2, 4, 12, 3};

    @Mock
    private HttpTableLookupFunction decorate;

    private AsyncHttpTableLookupFunction asyncFunction;

    private Set<String> threadNames;

    private CyclicBarrier barrier;

    @BeforeEach
    public void setUp() throws Exception {
        when(decorate.getOptions()).thenReturn(HttpLookupConfig.builder().build());

        barrier = new CyclicBarrier(rowKeys.length);
        threadNames = Collections.synchronizedSet(new HashSet<>());
        asyncFunction = new AsyncHttpTableLookupFunction(decorate);
        asyncFunction.open(Mockito.mock(FunctionContext.class));
    }

    @Test
    void shouldEvaluateInAsyncWay() throws InterruptedException {
        mockPolling();

        final List<RowData> result = Collections.synchronizedList(new ArrayList<>());

        CountDownLatch latch = new CountDownLatch(rowKeys.length);

        for (int key : rowKeys) {
            CompletableFuture<Collection<RowData>> future = new CompletableFuture<>();
            asyncFunction.eval(future, key);
            future.whenComplete(
                (rs, t) -> {
                    result.addAll(rs);
                    latch.countDown();
                });
        }

        assertThat(latch.await(3, TimeUnit.SECONDS))
            .withFailMessage(
                "Future complete in AsyncHttpTableLookupFunction was not called"
                    + " for at lest one event.")
            .isEqualTo(true);

        assertThat(result.size()).isEqualTo(rowKeys.length);
        assertThat(threadNames.size()).isEqualTo(rowKeys.length);
        verify(decorate, times(rowKeys.length)).lookup(any());
    }

    @Test
    void shouldHandleExceptionOnOneThread() throws InterruptedException {
        mockPollingWithException();

        final List<RowData> result = Collections.synchronizedList(new ArrayList<>());

        CountDownLatch latch = new CountDownLatch(rowKeys.length);
        AtomicBoolean wasException = new AtomicBoolean(false);

        for (int key : rowKeys) {
            CompletableFuture<Collection<RowData>> future = new CompletableFuture<>();
            asyncFunction.eval(future, key);
            future
                .whenComplete(
                    (rs, t) -> {
                        if (t != null) {
                            log.error(t.getMessage(), t);
                        }

                        result.addAll(rs);
                        latch.countDown();
                    })
                .exceptionally(
                    throwable -> {
                        wasException.set(true);
                        latch.countDown();
                        return emptyList();
                    });
        }

        assertThat(latch.await(3, TimeUnit.SECONDS))
            .withFailMessage(
                "Future complete in AsyncHttpTableLookupFunction was not called"
                    + " for at lest one event.")
            .isEqualTo(true);

        assertThat(wasException).isTrue();

        // -1 since one will have an exception
        assertThat(result.size()).isEqualTo(rowKeys.length - 1);
        assertThat(threadNames.size()).isEqualTo(rowKeys.length);
        verify(decorate, times(rowKeys.length)).lookup(any());
    }

    @Test
    void shouldHandleEmptyCollectionResult() throws InterruptedException {
        mockPollingWithEmptyList();

        final List<RowData> result = Collections.synchronizedList(new ArrayList<>());

        CountDownLatch latch = new CountDownLatch(rowKeys.length);
        AtomicInteger completeCount = new AtomicInteger(0);

        for (int key : rowKeys) {
            CompletableFuture<Collection<RowData>> future = new CompletableFuture<>();
            asyncFunction.eval(future, key);
            future
                .whenComplete(
                    (rs, t) -> {
                        if (t != null) {
                            log.error(t.getMessage(), t);
                        }

                        completeCount.incrementAndGet();
                        result.addAll(rs);
                        latch.countDown();
                    });
        }

        assertThat(latch.await(3, TimeUnit.SECONDS))
            .withFailMessage(
                "Future complete in AsyncHttpTableLookupFunction was not called"
                    + " for at lest one event.")
            .isEqualTo(true);

        assertThat(completeCount.get())
            .withFailMessage(
                "Future complete in AsyncHttpTableLookupFunction was not called"
                    + " for at lest one event.")
            .isEqualTo(rowKeys.length);

        // -1 since one will have one empty result.
        assertThat(result.size()).isEqualTo(rowKeys.length - 1);
        assertThat(threadNames.size()).isEqualTo(rowKeys.length);
        verify(decorate, times(rowKeys.length)).lookup(any());
    }

    private void mockPolling() {
        when(decorate.lookup(any()))
            .thenAnswer(
                invocationOnMock -> {
                    threadNames.add(Thread.currentThread().getName());
                    // make sure we pile up all keyRows on threads
                    barrier.await();
                    return singletonList(buildGenericRowData(
                        singletonList(invocationOnMock.getArgument(0))));
                });
    }

    private void mockPollingWithException() {
        when(decorate.lookup(any()))
            .thenAnswer(
                invocationOnMock -> {
                    threadNames.add(Thread.currentThread().getName());
                    // make sure we pile up all keyRows on threads
                    barrier.await();
                    Integer argument = ((GenericRowData) invocationOnMock.getArgument(0)).getInt(0);
                    if (argument == 12) {
                        throw new RuntimeException("Exception On problematic item");
                    }
                    return singletonList(buildGenericRowData(singletonList(argument)));
                });
    }

    private void mockPollingWithEmptyList() {
        when(decorate.lookup(any()))
            .thenAnswer(
                invocationOnMock -> {
                    threadNames.add(Thread.currentThread().getName());
                    // make sure we pile up all keyRows on threads
                    barrier.await();
                    Integer argument = ((GenericRowData) invocationOnMock.getArgument(0)).getInt(0);
                    if (argument == 12) {
                        return emptyList();
                    }
                    return singletonList(buildGenericRowData(singletonList(argument)));
                });
    }
}
