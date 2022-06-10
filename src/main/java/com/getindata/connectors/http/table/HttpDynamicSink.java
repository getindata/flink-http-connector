package com.getindata.connectors.http.table;

import com.getindata.connectors.http.sink.HttpSink;
import com.getindata.connectors.http.sink.HttpSinkBuilder;
import com.getindata.connectors.http.sink.HttpSinkRequestEntry;
import com.getindata.connectors.http.sink.httpclient.JavaNetSinkHttpClient;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.connector.base.table.sink.AsyncDynamicTableSink;
import org.apache.flink.connector.base.table.sink.AsyncDynamicTableSinkBuilder;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkV2Provider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;
import java.util.Objects;

/**
 * A dynamic HTTP Sink based on {@link AsyncDynamicTableSink} that adds Table API support for {@link HttpSink}.
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
 *   <li>{@code maxBufferSizeInBytes}: the maximum size of a batch of entries that may be sent to
 *       the HTTP endpoint measured in bytes;</li>
 *   <li>{@code maxTimeInBufferMS}: the maximum amount of time an entry is allowed to live in the
 *       buffer, if any element reaches this age, the entire buffer will be flushed
 *       immediately;</li>
 *   <li>{@code consumedDataType}: the consumed data type of the table;</li>
 *   <li>{@code encodingFormat}: the format for encoding records;</li>
 *   <li>{@code sinkConfig}: the configuration specific for the HTTP Sink.</li>
 * </ul>
 */
public class HttpDynamicSink extends AsyncDynamicTableSink<HttpSinkRequestEntry> {
  private final DataType consumedDataType;
  private final EncodingFormat<SerializationSchema<RowData>> encodingFormat;
  private final HttpDynamicSinkConfig sinkConfig;

  protected HttpDynamicSink(
      @Nullable Integer maxBatchSize,
      @Nullable Integer maxInFlightRequests,
      @Nullable Integer maxBufferedRequests,
      @Nullable Long maxBufferSizeInBytes,
      @Nullable Long maxTimeInBufferMS,
      DataType consumedDataType,
      EncodingFormat<SerializationSchema<RowData>> encodingFormat,
      HttpDynamicSinkConfig sinkConfig
  ) {
    super(maxBatchSize, maxInFlightRequests, maxBufferedRequests, maxBufferSizeInBytes, maxTimeInBufferMS);
    this.consumedDataType = Preconditions.checkNotNull(consumedDataType, "Consumed data type must not be null");
    this.encodingFormat = Preconditions.checkNotNull(encodingFormat, "Encoding format must not be null");
    this.sinkConfig = Preconditions.checkNotNull(sinkConfig, "HTTP Dynamic Sink Config must not be null");
  }

  @Override
  public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
    return encodingFormat.getChangelogMode();
  }

  @Override
  public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
    SerializationSchema<RowData> serializationSchema = encodingFormat.createRuntimeEncoder(context, consumedDataType);

    var insertMethod = sinkConfig.getInsertMethod();
    var contentType = sinkConfig.getContentType();
    HttpSinkBuilder<RowData> builder = HttpSink
        .<RowData>builder()
        .setEndpointUrl(sinkConfig.getUrl())
        .setSinkHttpClientBuilder(JavaNetSinkHttpClient::new)
        .setElementConverter((rowData, _context) -> new HttpSinkRequestEntry(
            insertMethod,
            contentType,
            serializationSchema.serialize(rowData)
        ));
    addAsyncOptionsToSinkBuilder(builder);

    return SinkV2Provider.of(builder.build());
  }

  @Override
  public DynamicTableSink copy() {
    return new HttpDynamicSink(
        maxBatchSize,
        maxInFlightRequests,
        maxBufferedRequests,
        maxBufferSizeInBytes,
        maxTimeInBufferMS,
        consumedDataType,
        encodingFormat,
        sinkConfig
    );
  }

  @Override
  public String asSummaryString() {
    return "HttpSink";
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    if (!super.equals(o)) return false;
    HttpDynamicSink that = (HttpDynamicSink) o;
    return Objects.equals(consumedDataType, that.consumedDataType) &&
           Objects.equals(encodingFormat, that.encodingFormat) &&
           Objects.equals(sinkConfig, that.sinkConfig);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), consumedDataType, encodingFormat, sinkConfig);
  }

  /** Builder to construct {@link HttpDynamicSink}. */
  public static class HttpDynamicTableSinkBuilder
      extends AsyncDynamicTableSinkBuilder<HttpSinkRequestEntry, HttpDynamicTableSinkBuilder> {

    private HttpDynamicSinkConfig sinkConfig = null;
    private DataType consumedDataType = null;
    private EncodingFormat<SerializationSchema<RowData>> encodingFormat = null;

    /**
     * @param sinkConfig the configuration specific for the HTTP Sink
     * @return {@link HttpDynamicTableSinkBuilder} itself
     */
    public HttpDynamicTableSinkBuilder setSinkConfig(HttpDynamicSinkConfig sinkConfig) {
      this.sinkConfig = sinkConfig;
      return this;
    }

    /**
     * @param encodingFormat the format for encoding records
     * @return {@link HttpDynamicTableSinkBuilder} itself
     */
    public HttpDynamicTableSinkBuilder setEncodingFormat(EncodingFormat<SerializationSchema<RowData>> encodingFormat) {
      this.encodingFormat = encodingFormat;
      return this;
    }

    /**
     * @param consumedDataType the consumed data type of the table
     * @return {@link HttpDynamicTableSinkBuilder} itself
     */
    public HttpDynamicTableSinkBuilder setConsumedDataType(DataType consumedDataType) {
      this.consumedDataType = consumedDataType;
      return this;
    }

    @Override
    public HttpDynamicSink build() {
      return new HttpDynamicSink(
          getMaxBatchSize(),
          getMaxInFlightRequests(),
          getMaxBufferedRequests(),
          getMaxBufferSizeInBytes(),
          getMaxTimeInBufferMS(),
          consumedDataType,
          encodingFormat,
          sinkConfig
      );
    }
  }
}
