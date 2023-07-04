package com.getindata.connectors.http.internal.sink.httpclient;

import java.util.List;

import lombok.Data;

@Data
public class HttpRequest {

    public final java.net.http.HttpRequest httpRequest;

    public final List<byte[]> elements;

    public final String method;

}
