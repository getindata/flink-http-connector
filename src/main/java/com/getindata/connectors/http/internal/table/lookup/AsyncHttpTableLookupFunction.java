package com.getindata.connectors.http.internal.table.lookup;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.AsyncLookupFunction;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;

import com.getindata.connectors.http.internal.config.HttpConnectorConfigConstants;
import com.getindata.connectors.http.internal.utils.ThreadUtils;

@Slf4j
@RequiredArgsConstructor
public class AsyncHttpTableLookupFunction extends AsyncLookupFunction {

    private static final String PULLING_THREAD_POOL_SIZE = "8";

    private static final String PUBLISHING_THREAD_POOL_SIZE = "4";

    /**
     * The {@link org.apache.flink.table.functions.TableFunction} we want to decorate with
     * async framework.
     */
    private final HttpTableLookupFunction decorate;

    /**
     * Thread pool for polling data from Http endpoint.
     */
    private transient ExecutorService pullingThreadPool;

    /**
     * Thread pool for publishing data to Flink.
     */
    private transient ExecutorService publishingThreadPool;

    @Override
    public void open(FunctionContext context) throws Exception {
        super.open(context);
        decorate.open(context);

        int pullingThreadPoolSize = Integer.parseInt(
            decorate.getOptions().getProperties().getProperty(
                HttpConnectorConfigConstants.LOOKUP_HTTP_PULING_THREAD_POOL_SIZE,
                PULLING_THREAD_POOL_SIZE)
        );

        int publishingThreadPoolSize = Integer.parseInt(
            decorate.getOptions().getProperties().getProperty(
                HttpConnectorConfigConstants.LOOKUP_HTTP_RESPONSE_THREAD_POOL_SIZE,
                PUBLISHING_THREAD_POOL_SIZE)
        );

        pullingThreadPool =
            Executors.newFixedThreadPool(
                pullingThreadPoolSize,
                new ExecutorThreadFactory(
                    "http-async-lookup-worker", ThreadUtils.LOGGING_EXCEPTION_HANDLER)
            );

        publishingThreadPool =
            Executors.newFixedThreadPool(
                publishingThreadPoolSize,
                new ExecutorThreadFactory(
                    "http-async-publishing-worker", ThreadUtils.LOGGING_EXCEPTION_HANDLER)
            );
    }

    @Override
    public CompletableFuture<Collection<RowData>> asyncLookup(RowData keyRow) {
        CompletableFuture<Collection<RowData>> future = new CompletableFuture<>();
        future.completeAsync(() -> decorate.lookup(keyRow), pullingThreadPool);

        // We don't want to use ForkJoinPool at all. We are using a different thread pool
        // for publishing here intentionally to avoid thread starvation.
        CompletableFuture<Collection<RowData>> resultFuture = new CompletableFuture<>();
        future.whenCompleteAsync(
            (result, throwable) -> {
                if (throwable != null) {
                    log.error("Exception while processing Http Async request", throwable);
                    resultFuture.completeExceptionally(
                        new RuntimeException("Exception while processing Http Async request",
                            throwable));
                } else {
                    resultFuture.complete(result);
                }
            },
            publishingThreadPool);
        return resultFuture;
    }

    public LookupRow getLookupRow() {
        return decorate.getLookupRow();
    }

    public HttpLookupConfig getOptions() {
        return decorate.getOptions();
    }

    @Override
    public void close() throws Exception {
        this.publishingThreadPool.shutdownNow();
        this.pullingThreadPool.shutdownNow();
        super.close();
    }
}
