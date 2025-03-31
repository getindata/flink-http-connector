package com.getindata.connectors.http;

import java.net.http.HttpResponse;

import lombok.Getter;

@Getter
public class HttpStatusCodeValidationFailedException extends Exception {
    private final HttpResponse<?> response;

    public HttpStatusCodeValidationFailedException(String message, HttpResponse<?> response) {
        super(message);
        this.response = response;
    }
}
