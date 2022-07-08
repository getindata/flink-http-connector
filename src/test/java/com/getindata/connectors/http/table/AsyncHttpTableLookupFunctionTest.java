package com.getindata.connectors.http.table;

import static com.getindata.connectors.http.internal.table.lookup.TableSourceHelper.buildGenericRowData;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.getindata.connectors.http.internal.table.lookup.AsyncHttpTableLookupFunction;
import com.getindata.connectors.http.internal.table.lookup.HttpTableLookupFunction;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.FunctionContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class AsyncHttpTableLookupFunctionTest {

  private final int[] rowKeys = {1, 2, 4, 12, 3};

  @Mock private HttpTableLookupFunction decorate;

  private AsyncHttpTableLookupFunction asyncFunction;

  private Set<String> threadNames;

  private CyclicBarrier barrier;

  @BeforeEach
  public void setUp() throws Exception {
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

    latch.await();

    assertThat(result.size()).isEqualTo(rowKeys.length);
    assertThat(threadNames.size()).isEqualTo(rowKeys.length);
    verify(decorate, times(rowKeys.length)).lookupByKeys(any());
  }

  @Test
  void shouldHandleExceptionOnOneThread() throws InterruptedException {
    mockPollingWithException();

    final List<RowData> result = Collections.synchronizedList(new ArrayList<>());

    CountDownLatch latch = new CountDownLatch(rowKeys.length);

    for (int key : rowKeys) {
      CompletableFuture<Collection<RowData>> future = new CompletableFuture<>();
      asyncFunction.eval(future, key);
      future
          .whenComplete(
              (rs, t) -> {
                if (t != null) {
                  System.out.println(t);
                }

                result.addAll(rs);
                latch.countDown();
              })
          .exceptionally(
              throwable -> {
                latch.countDown();
                return Collections.emptyList();
              });
    }

    latch.await();
  }

  private void mockPolling() {
    when(decorate.lookupByKeys(any()))
        .thenAnswer(
            invocationOnMock -> {
              threadNames.add(Thread.currentThread().getName());
              // make sure we pile up all keyRows on threads
              barrier.await();
              return buildGenericRowData(
                  Collections.singletonList(invocationOnMock.getArgument(0)));
            });
  }

  private void mockPollingWithException() {
    when(decorate.lookupByKeys(any()))
        .thenAnswer(
            invocationOnMock -> {
              threadNames.add(Thread.currentThread().getName());
              // make sure we pile up all keyRows on threads
              barrier.await();
              Integer argument = (Integer) ((Object[]) invocationOnMock.getArgument(0))[0];
              if (argument == 12) {
                throw new RuntimeException("Exception On problematic item");
              }
              return buildGenericRowData(Collections.singletonList(argument));
            });
  }
}
