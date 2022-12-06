package com.getindata.connectors.http.internal.sink;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.connector.base.sink.writer.AsyncSinkWriter;
import org.apache.flink.connector.base.sink.writer.BufferedRequestState;
import org.apache.flink.connector.base.sink.writer.ElementConverter;
import org.apache.flink.metrics.Counter;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;

import com.getindata.connectors.http.internal.SinkHttpClient;
import com.getindata.connectors.http.internal.config.HttpConnectorConfigConstants;
import com.getindata.connectors.http.internal.utils.ThreadUtils;

/**
 * Sink writer created by {@link com.getindata.connectors.http.HttpSink} to write to an HTTP
 * endpoint.
 *
 * <p>More details on the internals of this sink writer may be found in {@link AsyncSinkWriter}
 * documentation.
 *
 * @param <InputT> type of the elements that should be sent through HTTP request.
 */
@Slf4j
public class HttpSinkWriter<InputT> extends AsyncSinkWriter<InputT, HttpSinkRequestEntry> {

    private static final String HTTP_SINK_WRITER_THREAD_POOL_SIZE = "4";

    /**
     * Thread pool to handle HTTP response from HTTP client.
     */
    private final ExecutorService sinkWriterThreadPool;

    private final String endpointUrl;

    private final SinkHttpClient sinkHttpClient;

    private final Counter numRecordsSendErrorsCounter;

    public HttpSinkWriter(
            ElementConverter<InputT, HttpSinkRequestEntry> elementConverter,
            Sink.InitContext context,
            int maxBatchSize,
            int maxInFlightRequests,
            int maxBufferedRequests,
            long maxBatchSizeInBytes,
            long maxTimeInBufferMS,
            long maxRecordSizeInBytes,
            String endpointUrl,
            SinkHttpClient sinkHttpClient,
            Collection<BufferedRequestState<HttpSinkRequestEntry>> bufferedRequestStates,
            Properties properties) {

        super(elementConverter, context, maxBatchSize, maxInFlightRequests, maxBufferedRequests,
            maxBatchSizeInBytes, maxTimeInBufferMS, maxRecordSizeInBytes, bufferedRequestStates);
        this.endpointUrl = endpointUrl;
        this.sinkHttpClient = sinkHttpClient;

        var metrics = context.metricGroup();
        this.numRecordsSendErrorsCounter = metrics.getNumRecordsSendErrorsCounter();

        int sinkWriterThreadPollSize = Integer.parseInt(properties.getProperty(
            HttpConnectorConfigConstants.SINK_HTTP_WRITER_THREAD_POOL_SIZE,
            HTTP_SINK_WRITER_THREAD_POOL_SIZE
        ));

        this.sinkWriterThreadPool =
            Executors.newFixedThreadPool(
                sinkWriterThreadPollSize,
                new ExecutorThreadFactory(
                    "http-sink-writer-worker", ThreadUtils.LOGGING_EXCEPTION_HANDLER));
    }

    // TODO: Reintroduce retries by adding backoff policy
    @Override
    protected void submitRequestEntries(
            List<HttpSinkRequestEntry> requestEntries,
            Consumer<List<HttpSinkRequestEntry>> requestResult) {
        var future = sinkHttpClient.putRequests(requestEntries, endpointUrl);
        future.whenCompleteAsync((response, err) -> {
            if (err != null) {
                int failedRequestsNumber = requestEntries.size();
                log.error(
                    "Http Sink fatally failed to write all {} requests",
                    failedRequestsNumber);
                numRecordsSendErrorsCounter.inc(failedRequestsNumber);

                // TODO: Make `HttpSinkInternal` retry the failed requests.
                //  Currently, it does not retry those at all, only adds their count
                //  to the `numRecordsSendErrors` metric. It is due to the fact we do not have
                //  a clear image how we want to do it, so it would be both efficient and correct.
                //requestResult.accept(requestEntries);
            } else if (response.getFailedRequests().size() > 0) {
                int failedRequestsNumber = response.getFailedRequests().size();
                log.error("Http Sink failed to write and will retry {} requests",
                    failedRequestsNumber);
                numRecordsSendErrorsCounter.inc(failedRequestsNumber);

                // TODO: Make `HttpSinkInternal` retry the failed requests. Currently,
                //  it does not retry those at all, only adds their count to the
                //  `numRecordsSendErrors` metric. It is due to the fact we do not have
                //  a clear image how we want to do it, so it would be both efficient and correct.

                //requestResult.accept(response.getFailedRequests());
                //} else {
                //requestResult.accept(Collections.emptyList());
                //}
            }
            requestResult.accept(Collections.emptyList());
        }, sinkWriterThreadPool);
    }

    @Override
    protected long getSizeInBytes(HttpSinkRequestEntry s) {
        return s.getSizeInBytes();
    }

    @Override
    public void close() {
        sinkWriterThreadPool.shutdownNow();
        super.close();
    }
}
