package com.getindata.connectors.http.internal.table.sink;

import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import lombok.extern.slf4j.Slf4j;

import com.getindata.connectors.http.HttpSinkPostRequestCallback;
import com.getindata.connectors.http.internal.sink.HttpSinkRequestEntry;

@Slf4j
public class Slf4jHttpSinkPostRequestCallback implements HttpSinkPostRequestCallback {
    @Override
    public void call(
        HttpResponse<String> response,
        HttpSinkRequestEntry requestEntry,
        String _endpointUrl,
        Map<String, String> _headerMap
    ) {
        log.info(
            "Got response for a request.\n  Request:\n    " +
                "Method: {}\n    Body: {}\n  Response: {}\n    Body: {}",
            requestEntry.method,
            new String(requestEntry.element, StandardCharsets.UTF_8),
            response.toString(),
            response.body()
        );
    }
}
