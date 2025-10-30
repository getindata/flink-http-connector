package com.getindata.connectors.http;

import java.util.List;

import lombok.Getter;

import com.getindata.connectors.http.internal.sink.httpclient.HttpRequest;

@Getter
public class BatchHttpStatusCodeValidationFailedException extends Exception {
    List<HttpRequest> failedRequests;

    public BatchHttpStatusCodeValidationFailedException(String message, List<HttpRequest> failedRequests) {
        super(message);
        this.failedRequests = failedRequests;
    }
}
