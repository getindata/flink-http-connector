package com.getindata.connectors.http;

public class PostRequestCallbackException extends Exception {
    public PostRequestCallbackException(String message) {
        super(message);
    }

    public PostRequestCallbackException(String message, Throwable cause) {
        super(message, cause);
    }
}
