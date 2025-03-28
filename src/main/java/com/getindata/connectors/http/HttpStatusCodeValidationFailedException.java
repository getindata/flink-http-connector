package com.getindata.connectors.http;

import lombok.Getter;

import java.net.http.HttpResponse;

@Getter
public class HttpStatusCodeValidationFailedException extends Exception {
    private final HttpResponse<?> response;

    public HttpStatusCodeValidationFailedException(String message, HttpResponse<?> response) {
        super(message);
        this.response = response;
    }
}
