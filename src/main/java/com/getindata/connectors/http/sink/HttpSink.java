package com.getindata.connectors.http.sink;

import org.apache.flink.connector.base.sink.AsyncSinkBase;
import org.apache.flink.connector.base.sink.writer.BufferedRequestState;
import org.apache.flink.connector.base.sink.writer.ElementConverter;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;

public class HttpSink<InputT> extends AsyncSinkBase<InputT, HttpSinkRequestEntry> {
  private final String endpointUrl;

  protected HttpSink(
      ElementConverter<InputT, HttpSinkRequestEntry> elementConverter,
      int maxBatchSize,
      int maxInFlightRequests,
      int maxBufferedRequests,
      long maxBatchSizeInBytes,
      long maxTimeInBufferMS,
      long maxRecordSizeInBytes,
      String endpointUrl
  ) {
    super(elementConverter, maxBatchSize, maxInFlightRequests, maxBufferedRequests, maxBatchSizeInBytes,
          maxTimeInBufferMS, maxRecordSizeInBytes
    );
    this.endpointUrl =
        Preconditions.checkNotNull(endpointUrl, "The endpoint URL must not be null when initializing HTTP Sink.");
    Preconditions.checkArgument(!endpointUrl.isEmpty(), "The endpoint URL must be set when initializing HTTP Sink.");
  }

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
        recoveredState
    );
  }

  @Override
  public SimpleVersionedSerializer<BufferedRequestState<HttpSinkRequestEntry>> getWriterStateSerializer() {
    return new HttpSinkWriterStateSerializer();
  }
}
