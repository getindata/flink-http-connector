package com.getindata.connectors.http.internal;

import java.util.List;

import lombok.Data;
import lombok.NonNull;
import lombok.ToString;

import com.getindata.connectors.http.internal.sink.HttpSinkRequestEntry;
import com.getindata.connectors.http.internal.sink.httpclient.HttpRequest;

/**
 * Data class holding {@link HttpSinkRequestEntry} instances that {@link SinkHttpClient} attempted
 * to write, divided into two lists &mdash; successful and failed ones.
 */
@Data
@ToString
public class SinkHttpClientResponse {

    /**
     * A list of successfully written requests.
     */
    @NonNull
    private final List<HttpRequest> successfulRequests;

    /**
     * A list of requests that {@link SinkHttpClient} failed to write.
     */
    @NonNull
    private final List<HttpRequest> failedRequests;
}
