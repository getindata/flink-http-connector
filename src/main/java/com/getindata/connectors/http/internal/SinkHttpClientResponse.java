package com.getindata.connectors.http.internal;

import java.util.List;
import java.util.stream.Collectors;

import lombok.Data;
import lombok.NonNull;
import lombok.ToString;

import com.getindata.connectors.http.internal.config.ResponseItemStatus;
import com.getindata.connectors.http.internal.sink.HttpSinkRequestEntry;
import com.getindata.connectors.http.internal.sink.httpclient.HttpRequest;

/**
 * Data class holding {@link HttpSinkRequestEntry} instances that {@link SinkHttpClient} attempted
 * to write.
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
                .filter(r -> r.getStatus().equals(ResponseItemStatus.SUCCESS))
                .map(ResponseItem::getRequest)
                .collect(Collectors.toList());
    }

    public List<HttpRequest> getFailedRequests() {
        return requests.stream()
                .filter(r -> r.getStatus().equals(ResponseItemStatus.FAILURE))
                .map(ResponseItem::getRequest)
                .collect(Collectors.toList());
    }

    public List<HttpRequest> getTemporalRequests() {
        return requests.stream()
                .filter(r -> r.getStatus().equals(ResponseItemStatus.TEMPORAL))
                .map(ResponseItem::getRequest)
                .collect(Collectors.toList());
    }

    public List<HttpRequest> getIgnoredRequests() {
        return requests.stream()
                .filter(r -> r.getStatus().equals(ResponseItemStatus.IGNORE))
                .map(ResponseItem::getRequest)
                .collect(Collectors.toList());
    }

    @Data
    @ToString
    public static class ResponseItem {
        private final HttpRequest request;
        private final ResponseItemStatus status;
    }
}
