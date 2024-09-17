package com.getindata.connectors.http.internal.sink.httpclient;

import java.util.Optional;

import lombok.Data;
import lombok.NonNull;

import com.getindata.connectors.http.internal.sink.HttpSinkRequestEntry;
import okhttp3.Response;

/**
 * A wrapper structure around an HTTP response, keeping a reference to a particular {@link
 * HttpSinkRequestEntry}. Used internally by the {@code HttpSinkWriter} to pass {@code
 * HttpSinkRequestEntry} along some other element that it is logically connected with.
 */
@Data
final class JavaNetHttpResponseWrapper {

    /**
     * A representation of a single {@link com.getindata.connectors.http.HttpSink} request.
     */
    @NonNull
    private final HttpRequest httpRequest;

    /**
     * A response to an HTTP request based on {@link HttpSinkRequestEntry}.
     */
    private final Response response;

    public Optional<Response> getResponse() {
        return Optional.ofNullable(response);
    }
}
