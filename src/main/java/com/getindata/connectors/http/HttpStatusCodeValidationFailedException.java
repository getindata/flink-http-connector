package com.getindata.connectors.http;

import java.io.IOException;
import java.net.http.HttpResponse;

import lombok.Getter;

@Getter
public class HttpStatusCodeValidationFailedException extends IOException {
    private final HttpResponse<?> response;

    public HttpStatusCodeValidationFailedException(String message, HttpResponse<?> response) {
        super(message);
        this.response = response;
    }
}
