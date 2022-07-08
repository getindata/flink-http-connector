package com.getindata.connectors.http.internal.sink;

import com.getindata.connectors.http.internal.SinkHttpClient;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.connector.base.sink.writer.AsyncSinkWriter;
import org.apache.flink.connector.base.sink.writer.BufferedRequestState;
import org.apache.flink.connector.base.sink.writer.ElementConverter;
import org.apache.flink.metrics.Counter;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;

/**
 * Sink writer created by {@link com.getindata.connectors.http.HttpSink} to write to an HTTP endpoint.
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

    var metrics = context.metricGroup();
    this.numRecordsSendErrorsCounter = metrics.getNumRecordsSendErrorsCounter();
  }

  // TODO: Reintroduce retries by adding backoff policy
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

        // TODO: Make `HttpSinkInternal` retry the failed requests. Currently, it does not retry those at all,
        //  only adds their count to the `numRecordsSendErrors` metric. It is due to the fact we do not have
        //  a clear image how we want to do it, so it would be both efficient and correct.
//        requestResult.accept(requestEntries);
      } else if (response.getFailedRequests().size() > 0) {
        var failedRequestsNumber = response.getFailedRequests().size();
        log.error("Http Sink failed to write and will retry {} requests", failedRequestsNumber);
        numRecordsSendErrorsCounter.inc(failedRequestsNumber);
        
        // TODO: Make `HttpSinkInternal` retry the failed requests. Currently, it does not retry those at all,
        //  only adds their count to the `numRecordsSendErrors` metric. It is due to the fact we do not have
        //  a clear image how we want to do it, so it would be both efficient and correct.
//        requestResult.accept(response.getFailedRequests());
//      } else {
//        requestResult.accept(Collections.emptyList());
//      }
      }
      requestResult.accept(Collections.emptyList());
    });
  }

  @Override
  protected long getSizeInBytes(HttpSinkRequestEntry s) {
    return s.getSizeInBytes();
  }
}
