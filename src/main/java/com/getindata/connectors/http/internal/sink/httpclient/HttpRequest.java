package com.getindata.connectors.http.internal.sink.httpclient;

import java.util.List;

import lombok.Data;
import okhttp3.Request;

@Data
public class HttpRequest {

    public final Request httpRequest;

    public final List<byte[]> elements;

    public final String method;

}
