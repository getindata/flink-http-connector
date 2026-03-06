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

    private List<HttpRequest> getRequestByStatus(final ResponseItemStatus status) {
        return requests.stream()
                .filter(r -> r.getStatus().equals(status))
                .map(ResponseItem::getRequest)
                .collect(Collectors.toList());
    }

    public List<HttpRequest> getSuccessfulRequests() {
        return getRequestByStatus(ResponseItemStatus.SUCCESS);
    }

    public List<HttpRequest> getFailedRequests() {
        return getRequestByStatus(ResponseItemStatus.FAILURE);
    }

    public List<HttpRequest> getTemporalRequests() {
        return getRequestByStatus(ResponseItemStatus.TEMPORAL);
    }

    public List<HttpRequest> getIgnoredRequests() {
        return getRequestByStatus(ResponseItemStatus.IGNORE);
    }

    @Data
    @ToString
    public static class ResponseItem {
        private final HttpRequest request;
        private final ResponseItemStatus status;
    }
}
