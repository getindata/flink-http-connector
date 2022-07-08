package com.getindata.connectors.http;

import com.getindata.connectors.http.internal.SinkHttpClientBuilder;
import com.getindata.connectors.http.internal.sink.HttpSinkInternal;
import com.getindata.connectors.http.internal.sink.HttpSinkRequestEntry;
import org.apache.flink.connector.base.sink.writer.ElementConverter;

/**
 * A public implementation for {@code HttpSink} that performs async requests against a specified
 * HTTP endpoint using the buffering protocol specified in
 * {@link org.apache.flink.connector.base.sink.AsyncSinkBase}.
 *
 * <p>
 * To create a new instance  of this class use {@link HttpSinkBuilder}. An example would be:
 * <pre>
 * HttpSink<String> httpSink =
 *     HttpSink.<String>builder()
 *             .setEndpointUrl("http://example.com/myendpoint")
 *             .setElementConverter(
 *                 (s, _context) -> new HttpSinkRequestEntry("POST", "text/plain", s.getBytes(StandardCharsets.UTF_8)))
 *             .build();
 * </pre>
 *
 * @param <InputT> type of the elements that should be sent through HTTP request.
 */
public class HttpSink<InputT> extends HttpSinkInternal<InputT> {

    HttpSink(
            ElementConverter<InputT, HttpSinkRequestEntry> elementConverter,
            int maxBatchSize,
            int maxInFlightRequests,
            int maxBufferedRequests,
            long maxBatchSizeInBytes,
            long maxTimeInBufferMS,
            long maxRecordSizeInBytes,
            String endpointUrl,
            SinkHttpClientBuilder sinkHttpClientBuilder) {
        super(elementConverter,
            maxBatchSize,
            maxInFlightRequests,
            maxBufferedRequests,
            maxBatchSizeInBytes,
            maxTimeInBufferMS,
            maxRecordSizeInBytes,
            endpointUrl,
            sinkHttpClientBuilder
        );
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
}
