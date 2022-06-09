package com.getindata.connectors.http.sink;

import org.apache.flink.connector.base.sink.AsyncSinkBaseBuilder;
import org.apache.flink.connector.base.sink.writer.ElementConverter;

import java.util.Optional;

public class HttpSinkBuilder<InputT> extends
    AsyncSinkBaseBuilder<InputT, HttpSinkRequestEntry, HttpSinkBuilder<InputT>> {
  private static final int DEFAULT_MAX_BATCH_SIZE = 500;
  private static final int DEFAULT_MAX_IN_FLIGHT_REQUESTS = 50;
  private static final int DEFAULT_MAX_BUFFERED_REQUESTS = 10_000;
  private static final long DEFAULT_MAX_BATCH_SIZE_IN_B = 5 * 1024 * 1024;
  private static final long DEFAULT_MAX_TIME_IN_BUFFER_MS = 5000;
  private static final long DEFAULT_MAX_RECORD_SIZE_IN_B = 1 * 1024 * 1024;

  private String endpointUrl;
  private ElementConverter<InputT, HttpSinkRequestEntry> elementConverter;

  HttpSinkBuilder() {
  }

  public HttpSinkBuilder<InputT> setEndpointUrl(String endpointUrl) {
    this.endpointUrl = endpointUrl;
    return this;
  }

  public HttpSinkBuilder<InputT> setElementConverter(ElementConverter<InputT, HttpSinkRequestEntry> elementConverter) {
    this.elementConverter = elementConverter;
    return this;
  }

  @Override
  public HttpSink<InputT> build() {
    return new HttpSink<>(
        elementConverter,
        Optional.ofNullable(getMaxBatchSize()).orElse(DEFAULT_MAX_BATCH_SIZE),
        Optional.ofNullable(getMaxInFlightRequests())
                .orElse(DEFAULT_MAX_IN_FLIGHT_REQUESTS),
        Optional.ofNullable(getMaxBufferedRequests()).orElse(DEFAULT_MAX_BUFFERED_REQUESTS),
        Optional.ofNullable(getMaxBatchSizeInBytes()).orElse(DEFAULT_MAX_BATCH_SIZE_IN_B),
        Optional.ofNullable(getMaxTimeInBufferMS()).orElse(DEFAULT_MAX_TIME_IN_BUFFER_MS),
        Optional.ofNullable(getMaxRecordSizeInBytes()).orElse(DEFAULT_MAX_RECORD_SIZE_IN_B),
        endpointUrl
    );
  }
}
