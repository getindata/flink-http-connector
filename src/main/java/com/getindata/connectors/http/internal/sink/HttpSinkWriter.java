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
import org.apache.flink.connector.base.sink.writer.config.AsyncSinkWriterConfiguration;
import org.apache.flink.connector.base.sink.writer.strategy.AIMDScalingStrategy;
import org.apache.flink.connector.base.sink.writer.strategy.CongestionControlRateLimitingStrategy;
import org.apache.flink.connector.base.sink.writer.strategy.RateLimitingStrategy;
import org.apache.flink.metrics.Counter;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;

import com.getindata.connectors.http.BatchHttpStatusCodeValidationFailedException;
import com.getindata.connectors.http.internal.SinkHttpClient;
import com.getindata.connectors.http.internal.SinkHttpClientResponse;
import com.getindata.connectors.http.internal.config.HttpConnectorConfigConstants;
import com.getindata.connectors.http.internal.config.ResponseItemStatus;
import com.getindata.connectors.http.internal.sink.httpclient.HttpRequest;
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

    private static final int AIMD_RATE_LIMITING_STRATEGY_INCREASE_RATE = 10;
    private static final double AIMD_RATE_LIMITING_STRATEGY_DECREASE_FACTOR = 0.99D;

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
        super(
            elementConverter,
            context,
            AsyncSinkWriterConfiguration.builder()
                .setMaxBatchSize(maxBatchSize)
                .setMaxBatchSizeInBytes(maxBatchSizeInBytes)
                .setMaxInFlightRequests(maxInFlightRequests)
                .setMaxBufferedRequests(maxBufferedRequests)
                .setMaxTimeInBufferMS(maxTimeInBufferMS)
                .setMaxRecordSizeInBytes(maxRecordSizeInBytes)
                .setRateLimitingStrategy(
                    buildRateLimitingStrategy(maxInFlightRequests, maxBatchSize))
                .build(),
            bufferedRequestStates);
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

    private static RateLimitingStrategy buildRateLimitingStrategy(
            int maxInFlightRequests, int maxBatchSize) {
        return CongestionControlRateLimitingStrategy.builder()
            .setMaxInFlightRequests(maxInFlightRequests)
            .setInitialMaxInFlightMessages(maxBatchSize)
            .setScalingStrategy(
                AIMDScalingStrategy.builder(maxBatchSize * maxInFlightRequests)
                    .setIncreaseRate(AIMD_RATE_LIMITING_STRATEGY_INCREASE_RATE)
                    .setDecreaseFactor(AIMD_RATE_LIMITING_STRATEGY_DECREASE_FACTOR)
                    .build())
            .build();
    }

    @Override
    protected void submitRequestEntries(
            List<HttpSinkRequestEntry> requestEntries,
            Consumer<List<HttpSinkRequestEntry>> requestResult) {
        var future = sinkHttpClient.putRequests(requestEntries, endpointUrl);
        future.whenCompleteAsync((response, err) -> {
            if (err != null) {
                handleFullyFailedRequest(err, requestEntries, requestResult);
            } else {
                List<HttpRequest> failedRequests = response.getFailedRequests();
                List<HttpRequest> ignoredRequests = response.getIgnoredRequests();
                List<HttpRequest> temporalRequests = response.getTemporalRequests();

                if (!failedRequests.isEmpty()) {
                    numRecordsSendErrorsCounter.inc(failedRequests.size());
                    log.error(
                            "failed requests: {}, throwing BatchHttpStatusCodeValidationFailedException from sink",
                            failedRequests
                    );
                    getFatalExceptionCons().accept(new BatchHttpStatusCodeValidationFailedException(
                            String.format("Received %d fatal response codes", failedRequests.size()), failedRequests)
                    );
                }

                if (!ignoredRequests.isEmpty()) {
                    log.info("Ignoring {} requests", ignoredRequests.size());
                }

                if (!temporalRequests.isEmpty()) {
                    numRecordsSendErrorsCounter.inc(temporalRequests.size());
                    if (deliveryGuarantee == DeliveryGuarantee.AT_LEAST_ONCE) {
                        log.warn("Retrying {} requests", temporalRequests.size());
                        handlePartiallyFailedRequest(response, requestEntries, requestResult);
                    } else if (deliveryGuarantee == DeliveryGuarantee.NONE) {
                        log.warn(
                            "Http Sink failed to write {} requests but will continue due to {} DeliveryGuarantee",
                            temporalRequests.size(),
                            deliveryGuarantee
                        );
                        requestResult.accept(Collections.emptyList());
                    } else {
                        throw new UnsupportedOperationException(
                                "Unsupported delivery guarantee: " + deliveryGuarantee);
                    }
                } else {
                    requestResult.accept(Collections.emptyList());
                }
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
        // Assumption: the order of response.requests is the same as requestEntries.
        // See com.getindata.connectors.http.internal.sink.httpclient.
        // JavaNetSinkHttpClient#putRequests where requests are submitted sequentially and
        // then their futures are joined sequentially too.
        List<HttpSinkRequestEntry> failedRequestEntries = new ArrayList<>();
        for (int i = 0; i < response.getRequests().size(); ++i) {
            if (response.getRequests().get(i).getStatus().equals(ResponseItemStatus.TEMPORAL)) {
                failedRequestEntries.add(requestEntries.get(i));
            }
        }
        requestResult.accept(failedRequestEntries);
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
