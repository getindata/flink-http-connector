package com.getindata.connectors.http.sink;

import com.getindata.connectors.http.SinkHttpClientBuilder;
import org.apache.flink.connector.base.sink.AsyncSinkBase;
import org.apache.flink.connector.base.sink.writer.BufferedRequestState;
import org.apache.flink.connector.base.sink.writer.ElementConverter;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StringUtils;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;

/**
 * An HTTP Sink that performs async requests against a specified HTTP endpoint using the buffering
 * protocol specified in {@link AsyncSinkBase}.
 *
 * <p>The behaviour of the buffering may be specified by providing configuration during the sink build time.
 *
 * <ul>
 *   <li>{@code maxBatchSize}: the maximum size of a batch of entries that may be sent to the HTTP
 *       endpoint;</li>
 *   <li>{@code maxInFlightRequests}: the maximum number of in flight requests that may exist, if
 *       any more in flight requests need to be initiated once the maximum has been reached, then it
 *       will be blocked until some have completed;</li>
 *   <li>{@code maxBufferedRequests}: the maximum number of elements held in the buffer, requests to
 *       add elements will be blocked while the number of elements in the buffer is at the
 *       maximum;</li>
 *   <li>{@code maxBatchSizeInBytes}: the maximum size of a batch of entries that may be sent to
 *       the HTTP endpoint measured in bytes;</li>
 *   <li>{@code maxTimeInBufferMS}: the maximum amount of time an entry is allowed to live in the
 *       buffer, if any element reaches this age, the entire buffer will be flushed
 *       immediately;</li>
 *   <li>{@code maxRecordSizeInBytes}: the maximum size of a record the sink will accept into the
 *       buffer, a record of size larger than this will be rejected when passed to the sink.</li>
 * </ul>
 *
 * @param <InputT> type of the elements that should be sent through HTTP request.
 */
public class HttpSink<InputT> extends AsyncSinkBase<InputT, HttpSinkRequestEntry> {
  private final String endpointUrl;

  // having Builder instead of an instance of `SinkHttpClient` makes it possible to serialize `HttpSink`
  private final SinkHttpClientBuilder sinkHttpClientBuilder;

  protected HttpSink(
      ElementConverter<InputT, HttpSinkRequestEntry> elementConverter,
      int maxBatchSize,
      int maxInFlightRequests,
      int maxBufferedRequests,
      long maxBatchSizeInBytes,
      long maxTimeInBufferMS,
      long maxRecordSizeInBytes,
      String endpointUrl,
      SinkHttpClientBuilder sinkHttpClientBuilder
  ) {
    super(elementConverter, maxBatchSize, maxInFlightRequests, maxBufferedRequests, maxBatchSizeInBytes,
          maxTimeInBufferMS, maxRecordSizeInBytes
    );
    Preconditions.checkArgument(!StringUtils.isNullOrWhitespaceOnly(endpointUrl), "The endpoint URL must be set when initializing HTTP Sink.");
    this.endpointUrl = endpointUrl;
    this.sinkHttpClientBuilder =
        Preconditions.checkNotNull(sinkHttpClientBuilder, "The HTTP client builder must not be null when initializing HTTP Sink.");
  }

  /**
   * Create a {@link HttpSinkBuilder} constructing a new {@link HttpSink}.
   *
   * @param <InputT> type of the elements that should be sent through HTTP request
   * @return {@link HttpSinkBuilder}
   */
  public static <InputT> HttpSinkBuilder<InputT> builder() {
    return new HttpSinkBuilder<>();
  }

  @Override
  public StatefulSinkWriter<InputT, BufferedRequestState<HttpSinkRequestEntry>> createWriter(
      InitContext context
  ) throws IOException {
    return new HttpSinkWriter<>(
        getElementConverter(),
        context,
        getMaxBatchSize(),
        getMaxInFlightRequests(),
        getMaxBufferedRequests(),
        getMaxBatchSizeInBytes(),
        getMaxTimeInBufferMS(),
        getMaxRecordSizeInBytes(),
        endpointUrl,
        sinkHttpClientBuilder.build(),
        Collections.emptyList()
    );
  }

  @Override
  public StatefulSinkWriter<InputT, BufferedRequestState<HttpSinkRequestEntry>> restoreWriter(
      InitContext context, Collection<BufferedRequestState<HttpSinkRequestEntry>> recoveredState
  ) throws IOException {
    return new HttpSinkWriter<>(
        getElementConverter(),
        context,
        getMaxBatchSize(),
        getMaxInFlightRequests(),
        getMaxBufferedRequests(),
        getMaxBatchSizeInBytes(),
        getMaxTimeInBufferMS(),
        getMaxRecordSizeInBytes(),
        endpointUrl,
        sinkHttpClientBuilder.build(),
        recoveredState
    );
  }

  @Override
  public SimpleVersionedSerializer<BufferedRequestState<HttpSinkRequestEntry>> getWriterStateSerializer() {
    return new HttpSinkWriterStateSerializer();
  }
}
