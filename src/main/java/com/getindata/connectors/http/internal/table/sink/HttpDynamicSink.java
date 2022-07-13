package com.getindata.connectors.http.internal.table.sink;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import javax.annotation.Nullable;

import lombok.EqualsAndHashCode;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.base.table.sink.AsyncDynamicTableSink;
import org.apache.flink.connector.base.table.sink.AsyncDynamicTableSinkBuilder;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkV2Provider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.Preconditions;

import com.getindata.connectors.http.HttpSink;
import com.getindata.connectors.http.HttpSinkBuilder;
import com.getindata.connectors.http.internal.sink.HttpSinkRequestEntry;
import com.getindata.connectors.http.internal.sink.httpclient.JavaNetSinkHttpClient;
import static com.getindata.connectors.http.internal.config.HttpConnectorConfigConstants.CONTENT_TYPE_HEADER;
import static com.getindata.connectors.http.internal.table.sink.HttpDynamicSinkConnectorOptions.INSERT_METHOD;
import static com.getindata.connectors.http.internal.table.sink.HttpDynamicSinkConnectorOptions.URL;

/**
 * A dynamic HTTP Sink based on {@link AsyncDynamicTableSink} that adds Table API support for {@link
 * HttpSink}.
 *
 * <p>The behaviour of the buffering may be specified by providing configuration during the sink
 * build time.
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
 *   <li>{@code formatContentTypeMap}: the mapping from 'format' field option to 'Content-Type'
 *   header value;</li>
 *   <li>{@code tableOptions}: the {@link ReadableConfig} instance with values defined in Table
 *   API DDL.</li>
 * </ul>
 *
 * <p>The following example shows the minimum Table API example to create a {@link HttpDynamicSink}
 * that writes JSON values to an HTTP endpoint using POST method.
 *
 * <pre>{@code
 * CREATE TABLE http (
 *   id bigint,
 *   some_field string
 * ) with (
 *   'connector' = 'http-sink'
 *   'url' = 'http://example.com/myendpoint'
 *   'format' = 'json'
 * )
 * }</pre>
 */
@Slf4j
@EqualsAndHashCode(callSuper = true)
public class HttpDynamicSink extends AsyncDynamicTableSink<HttpSinkRequestEntry> {

    private final DataType consumedDataType;

    private final EncodingFormat<SerializationSchema<RowData>> encodingFormat;

    private final ReadableConfig tableOptions;

    private final Map<String, String> formatContentTypeMap;

    private final Properties properties;

    protected HttpDynamicSink(
        @Nullable Integer maxBatchSize,
        @Nullable Integer maxInFlightRequests,
        @Nullable Integer maxBufferedRequests,
        @Nullable Long maxBufferSizeInBytes,
        @Nullable Long maxTimeInBufferMS,
        DataType consumedDataType,
        EncodingFormat<SerializationSchema<RowData>> encodingFormat,
        Map<String, String> formatContentTypeMap,
        ReadableConfig tableOptions,
        Properties properties
    ) {
        super(maxBatchSize, maxInFlightRequests, maxBufferedRequests, maxBufferSizeInBytes,
            maxTimeInBufferMS);
        this.consumedDataType =
            Preconditions.checkNotNull(consumedDataType, "Consumed data type must not be null");
        this.encodingFormat =
            Preconditions.checkNotNull(encodingFormat, "Encoding format must not be null");
        this.formatContentTypeMap = Preconditions.checkNotNull(formatContentTypeMap,
            "Format to content type map must not be null");
        this.tableOptions =
            Preconditions.checkNotNull(tableOptions, "Table options must not be null");
        this.properties = properties;
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        return encodingFormat.getChangelogMode();
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        SerializationSchema<RowData> serializationSchema =
            encodingFormat.createRuntimeEncoder(context, consumedDataType);

        var insertMethod = tableOptions.get(INSERT_METHOD);
        var contentType = getContentTypeFromFormat(tableOptions.get(FactoryUtil.FORMAT));

        // TODO ESP-98 add headers to DDL and add tests for this
        HttpSinkBuilder<RowData> builder = HttpSink
            .<RowData>builder()
            .setEndpointUrl(tableOptions.get(URL))
            .setSinkHttpClientBuilder(JavaNetSinkHttpClient::new)
            .setElementConverter((rowData, _context) -> new HttpSinkRequestEntry(
                insertMethod,
                serializationSchema.serialize(rowData)
            ))
            .setProperty(CONTENT_TYPE_HEADER, contentType)
            .setProperties(properties);
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
            new HashMap<>(formatContentTypeMap),
            tableOptions,
            properties
        );
    }

    @Override
    public String asSummaryString() {
        return "HttpSink";
    }

    private String getContentTypeFromFormat(String format) {
        var contentType = formatContentTypeMap.get(format);
        if (contentType != null) {
            return contentType;
        }

        log.warn(
            "Unexpected format {}. MIME type for the request will be set to \"application/{}\".",
            format, format);
        return "application/" + format;
    }

    /**
     * Builder to construct {@link HttpDynamicSink}.
     */
    public static class HttpDynamicTableSinkBuilder
        extends AsyncDynamicTableSinkBuilder<HttpSinkRequestEntry, HttpDynamicTableSinkBuilder> {

        private final Properties properties = new Properties();

        private ReadableConfig tableOptions;

        private Map<String, String> formatContentTypeMap;

        private DataType consumedDataType;

        private EncodingFormat<SerializationSchema<RowData>> encodingFormat;

        /**
         * @param tableOptions the {@link ReadableConfig} consisting of options listed in table
         *                     creation DDL
         * @return {@link HttpDynamicTableSinkBuilder} itself
         */
        public HttpDynamicTableSinkBuilder setTableOptions(ReadableConfig tableOptions) {
            this.tableOptions = tableOptions;
            return this;
        }

        public HttpDynamicTableSinkBuilder setFormatContentTypeMap(
            Map<String, String> formatContentTypeMap) {
            this.formatContentTypeMap = formatContentTypeMap;
            return this;
        }

        /**
         * @param encodingFormat the format for encoding records
         * @return {@link HttpDynamicTableSinkBuilder} itself
         */
        public HttpDynamicTableSinkBuilder setEncodingFormat(
            EncodingFormat<SerializationSchema<RowData>> encodingFormat) {
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

        /**
         * Set property for Http Sink.
         * @param propertyName property name.
         * @param propertyValue property value.
         */
        public HttpDynamicTableSinkBuilder setProperty(String propertyName, String propertyValue) {
            this.properties.setProperty(propertyName, propertyValue);
            return this;
        }

        /**
         * Add properties to Http Sink configuration
         * @param properties Properties to add.
         */
        public HttpDynamicTableSinkBuilder setProperties(Properties properties) {
            this.properties.putAll(properties);
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
                formatContentTypeMap,
                tableOptions,
                properties
            );
        }
    }
}
