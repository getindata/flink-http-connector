package com.getindata.connectors.http.internal.sink;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.base.sink.writer.AsyncSinkWriter;
import org.apache.flink.connector.base.sink.writer.BufferedRequestState;
import org.apache.flink.connector.base.sink.writer.ElementConverter;
import org.apache.flink.metrics.Counter;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;

import com.getindata.connectors.http.internal.SinkHttpClient;
import com.getindata.connectors.http.internal.SinkHttpClientResponse;
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

    private final DeliveryGuarantee deliveryGuarantee;

    public HttpSinkWriter(
            ElementConverter<InputT, HttpSinkRequestEntry> elementConverter,
            Sink.InitContext context,
            int maxBatchSize,
            int maxInFlightRequests,
            int maxBufferedRequests,
            long maxBatchSizeInBytes,
            long maxTimeInBufferMS,
            long maxRecordSizeInBytes,
            DeliveryGuarantee deliveryGuarantee,
            String endpointUrl,
            SinkHttpClient sinkHttpClient,
            Collection<BufferedRequestState<HttpSinkRequestEntry>> bufferedRequestStates,
            Properties properties) {

        super(elementConverter, context, maxBatchSize, maxInFlightRequests, maxBufferedRequests,
            maxBatchSizeInBytes, maxTimeInBufferMS, maxRecordSizeInBytes, bufferedRequestStates);
        this.deliveryGuarantee = deliveryGuarantee;
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

        this.sinkHttpClient.open();
    }

    // TODO: Reintroduce retries by adding backoff policy
    @Override
    protected void submitRequestEntries(
            List<HttpSinkRequestEntry> requestEntries,
            Consumer<List<HttpSinkRequestEntry>> requestResult) {
        var future = sinkHttpClient.putRequests(requestEntries, endpointUrl);
        future.whenCompleteAsync((response, err) -> {
            if (err != null) {
                handleFullyFailedRequest(err, requestEntries, requestResult);
            } else if (response.getFailedRequests().size() > 0) {
                int failedRequestsNumber = response.getFailedRequests().size();
                numRecordsSendErrorsCounter.inc(failedRequestsNumber);
                log.error("Http Sink failed to write and will retry {} requests",
                        failedRequestsNumber);
                handlePartiallyFailedRequest(response, requestEntries, requestResult);
            } else {
                requestResult.accept(Collections.emptyList());
            }
        }, sinkWriterThreadPool);
    }

    private void handleFullyFailedRequest(Throwable err,
                                          List<HttpSinkRequestEntry> requestEntries,
                                          Consumer<List<HttpSinkRequestEntry>> requestResult) {
        int failedRequestsNumber = requestEntries.size();
        numRecordsSendErrorsCounter.inc(failedRequestsNumber);

        if (deliveryGuarantee == DeliveryGuarantee.AT_LEAST_ONCE) {
            // Retry all requests.
            log.error("Http Sink fatally failed to write and will retry {} requests", failedRequestsNumber, err);
            requestResult.accept(requestEntries);
        } else if (deliveryGuarantee == DeliveryGuarantee.NONE) {
            // Do not retry failed requests.
            log.error(
                    "Http Sink fatally failed to write {} requests but will continue due to {} DeliveryGuarantee",
                    failedRequestsNumber,
                    deliveryGuarantee,
                    err
            );
            requestResult.accept(Collections.emptyList());
        } else {
            throw new UnsupportedOperationException(
                    "Unsupported delivery guarantee: " + deliveryGuarantee);
        }
    }

    private void handlePartiallyFailedRequest(SinkHttpClientResponse response,
                                              List<HttpSinkRequestEntry> requestEntries,
                                              Consumer<List<HttpSinkRequestEntry>> requestResult) {
        if (deliveryGuarantee == DeliveryGuarantee.AT_LEAST_ONCE) {
            // Assumption: the order of response.requests is the same as requestEntries.
            // See com.getindata.connectors.http.internal.sink.httpclient.
            // JavaNetSinkHttpClient#putRequests where requests are submitted sequentially and
            // then their futures are joined sequentially too.
            List<HttpSinkRequestEntry> failedRequestEntries = new ArrayList<>();
            for (int i = 0; i < response.getRequests().size(); ++i) {
                if (!response.getRequests().get(i).isSuccessful()) {
                    failedRequestEntries.add(requestEntries.get(i));
                }
            }
            requestResult.accept(failedRequestEntries);
        } else if (deliveryGuarantee == DeliveryGuarantee.NONE) {
            // Do not retry failed requests.
            requestResult.accept(Collections.emptyList());
        }
    }

    @Override
    protected long getSizeInBytes(HttpSinkRequestEntry s) {
        return s.getSizeInBytes();
    }

    @Override
    public void close() {
        sinkHttpClient.close();
        sinkWriterThreadPool.shutdownNow();
        super.close();
    }
}
