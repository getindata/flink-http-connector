package com.getindata.connectors.http.internal.sink;

import java.io.Serializable;

import lombok.EqualsAndHashCode;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;


/**
 * Represents a single {@link com.getindata.connectors.http.HttpSink} request. Contains the HTTP
 * method name, Content-Type header value, and byte representation of the body of the request.
 */
@RequiredArgsConstructor
@EqualsAndHashCode
public final class HttpSinkRequestEntry implements Serializable {

    /**
     * HTTP method name to use when sending the request.
     */
    @NonNull
    public final String method;

    /**
     * Body of the request, encoded as byte array.
     */
    public final byte[] element;

    /**
     * @return the size of the {@link HttpSinkRequestEntry#element}
     */
    public long getSizeInBytes() {
        return element.length;
    }
}
