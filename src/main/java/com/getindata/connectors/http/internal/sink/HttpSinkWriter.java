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

    // TODO: Reintroduce retries by adding backoff policy
    @Override
    protected void submitRequestEntries(
            List<HttpSinkRequestEntry> requestEntries,
            Consumer<List<HttpSinkRequestEntry>> requestResult) {
        var future = sinkHttpClient.putRequests(requestEntries, endpointUrl);
        future.whenCompleteAsync((response, err) -> {
            if (err != null) {
                handleFullyFailedRequest(err, requestEntries, requestResult);
            } else if (response.getRequests().stream().anyMatch(r -> !r.isSuccessful())) {
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
        log.error(
                "Http Sink fatally failed to write all {} requests",
                failedRequestsNumber);
        numRecordsSendErrorsCounter.inc(failedRequestsNumber);

        if (deliveryGuarantee == DeliveryGuarantee.AT_LEAST_ONCE) {
            // Retry all requests.
            requestResult.accept(requestEntries);
        } else if (deliveryGuarantee == DeliveryGuarantee.NONE) {
            // Do not retry failed requests.
            requestResult.accept(Collections.emptyList());
        }
    }

    private void handlePartiallyFailedRequest(SinkHttpClientResponse response,
                                              List<HttpSinkRequestEntry> requestEntries,
                                              Consumer<List<HttpSinkRequestEntry>> requestResult) {
        long failedRequestsNumber = response.getRequests().stream()
                .filter(r -> !r.isSuccessful())
                .count();
        log.error("Http Sink failed to write and will retry {} requests",
                failedRequestsNumber);
        numRecordsSendErrorsCounter.inc(failedRequestsNumber);

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
        sinkWriterThreadPool.shutdownNow();
        super.close();
    }
}
