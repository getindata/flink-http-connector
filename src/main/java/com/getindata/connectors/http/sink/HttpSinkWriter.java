package com.getindata.connectors.http.sink;

import com.getindata.connectors.http.SinkHttpClient;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.connector.base.sink.writer.AsyncSinkWriter;
import org.apache.flink.connector.base.sink.writer.BufferedRequestState;
import org.apache.flink.connector.base.sink.writer.ElementConverter;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;

/**
 * Sink writer created by {@link HttpSink} to write to an HTTP endpoint.
 *
 * <p>More details on the internals of this sink writer may be found in {@link AsyncSinkWriter}
 * documentation.
 *
 * @param <InputT> type of the elements that should be sent through HTTP request.
 */
@Slf4j
public class HttpSinkWriter<InputT> extends AsyncSinkWriter<InputT, HttpSinkRequestEntry> {
  private final String endpointUrl;
  private final SinkHttpClient sinkHttpClient;
  private final SinkWriterMetricGroup metrics;
  private final Counter numRecordsSendErrorsCounter;

  public HttpSinkWriter(
      ElementConverter<InputT, HttpSinkRequestEntry> elementConverter, Sink.InitContext context, int maxBatchSize,
      int maxInFlightRequests, int maxBufferedRequests, long maxBatchSizeInBytes, long maxTimeInBufferMS,
      long maxRecordSizeInBytes, String endpointUrl, SinkHttpClient sinkHttpClient
  ) {
    this(elementConverter, context, maxBatchSize, maxInFlightRequests, maxBufferedRequests, maxBatchSizeInBytes,
         maxTimeInBufferMS, maxRecordSizeInBytes, endpointUrl, sinkHttpClient, Collections.emptyList()
    );
  }

  public HttpSinkWriter(
      ElementConverter<InputT, HttpSinkRequestEntry> elementConverter, Sink.InitContext context, int maxBatchSize,
      int maxInFlightRequests, int maxBufferedRequests, long maxBatchSizeInBytes, long maxTimeInBufferMS,
      long maxRecordSizeInBytes, String endpointUrl, SinkHttpClient sinkHttpClient,
      Collection<BufferedRequestState<HttpSinkRequestEntry>> bufferedRequestStates
  ) {
    super(elementConverter, context, maxBatchSize, maxInFlightRequests, maxBufferedRequests, maxBatchSizeInBytes,
          maxTimeInBufferMS, maxRecordSizeInBytes, bufferedRequestStates
    );
    this.endpointUrl = endpointUrl;
    this.sinkHttpClient = sinkHttpClient;
    this.metrics = context.metricGroup();
    this.numRecordsSendErrorsCounter = metrics.getNumRecordsSendErrorsCounter();
  }

  @Override
  protected void submitRequestEntries(
      List<HttpSinkRequestEntry> requestEntries, Consumer<List<HttpSinkRequestEntry>> requestResult
  ) {
    var future = sinkHttpClient.putRequests(requestEntries, endpointUrl);
    future.whenComplete((response, err) -> {
      if (err != null) {
        var failedRequestsNumber = requestEntries.size();
        log.error("Http Sink fatally failed to write all {} requests", failedRequestsNumber);
        numRecordsSendErrorsCounter.inc(failedRequestsNumber);
        requestResult.accept(requestEntries);
      } else if (response.getFailedRequests().size() > 0) {
        var failedRequestsNumber = response.getFailedRequests().size();
        log.error("Http Sink failed to write and will retry {} requests", failedRequestsNumber);
        numRecordsSendErrorsCounter.inc(failedRequestsNumber);
        requestResult.accept(response.getFailedRequests());
      } else {
        requestResult.accept(Collections.emptyList());
      }
    });
  }

  @Override
  protected long getSizeInBytes(HttpSinkRequestEntry s) {
    return s.getSizeInBytes();
  }
}
