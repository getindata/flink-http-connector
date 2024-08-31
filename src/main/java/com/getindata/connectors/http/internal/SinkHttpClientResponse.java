package com.getindata.connectors.http.internal;

import java.util.List;
import java.util.stream.Collectors;

import lombok.Data;
import lombok.NonNull;
import lombok.ToString;

import com.getindata.connectors.http.internal.sink.HttpSinkRequestEntry;
import com.getindata.connectors.http.internal.sink.httpclient.HttpRequest;
import com.getindata.connectors.http.internal.status.HttpResponseStatus;

/**
 * Data class holding {@link HttpSinkRequestEntry} instances that {@link SinkHttpClient} attempted
 * to write, divided into two lists &mdash; successful and failed ones.
 */
@Data
@ToString
public class SinkHttpClientResponse {

    /**
     * A list of requests along with write status.
     */
    @NonNull
    private final List<ResponseItem> requests;

    public List<HttpRequest> getSuccessfulRequests() {
        return requests.stream()
                .filter(i -> i.getStatus() == HttpResponseStatus.SUCCESS)
                .map(ResponseItem::getRequest)
                .collect(Collectors.toList());
    }

    public List<HttpRequest> getFailedRetryableRequests() {
        return requests.stream()
                .filter(i -> i.getStatus() == HttpResponseStatus.FAILURE_RETRYABLE)
                .map(ResponseItem::getRequest)
                .collect(Collectors.toList());
    }

    public List<HttpRequest> getFailedNotRetryableRequests() {
        return requests.stream()
                .filter(i -> i.getStatus() == HttpResponseStatus.FAILURE_NOT_RETRYABLE)
                .map(ResponseItem::getRequest)
                .collect(Collectors.toList());
    }

    @Data
    @ToString
    public static class ResponseItem {
        private final HttpRequest request;
        private final HttpResponseStatus status;
    }
}
