package com.getindata.connectors.http.internal.table.lookup;

import com.getindata.connectors.http.internal.table.lookup.HttpTableLookupFunction.ColumnData;
import com.getindata.connectors.http.internal.utils.ThreadUtils;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.AsyncTableFunction;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;

@Slf4j
@RequiredArgsConstructor
public class AsyncHttpTableLookupFunction extends AsyncTableFunction<RowData> {

  private static final int POLLING_THREAD_POOL_SIZE = 8;

  private static final int PUBLISHING_THREAD_POOL_SIZE = 4;

  private final HttpTableLookupFunction decorate;

  private transient ExecutorService pollingThreadPool;

  private transient ExecutorService publishingThreadPool;

  @Override
  public void open(FunctionContext context) throws Exception {
    super.open(context);
    decorate.open(context);

    pollingThreadPool =
        Executors.newFixedThreadPool(
            POLLING_THREAD_POOL_SIZE,
            new ExecutorThreadFactory(
                "http-async-lookup-worker", ThreadUtils.LOGGING_EXCEPTION_HANDLER));

    publishingThreadPool =
        Executors.newFixedThreadPool(
            PUBLISHING_THREAD_POOL_SIZE,
            new ExecutorThreadFactory(
                "http-async-publishing-worker", ThreadUtils.LOGGING_EXCEPTION_HANDLER));
  }

  public void eval(CompletableFuture<Collection<RowData>> resultFuture, Object... keys) {

    CompletableFuture<RowData> future = new CompletableFuture<>();
    future.completeAsync(() -> decorate.lookupByKeys(keys), pollingThreadPool);

    // We don't want to use ForkJoinPool at all. We are using a different thread pool for publishing
    // here intentionally to avoid thread starvation.
    future.whenCompleteAsync(
        (result, throwable) -> {
          if (throwable != null) {
            log.error("Exception while processing Http Async request", throwable);
            resultFuture.completeExceptionally(
                new RuntimeException("Exception while processing Http Async request", throwable));
          } else {
            resultFuture.complete(Collections.singleton(result));
          }
        },
        publishingThreadPool);
  }

  public ColumnData getColumnData() {
    return decorate.getColumnData();
  }

  public HttpLookupConfig getOptions() {
    return decorate.getOptions();
  }
}
