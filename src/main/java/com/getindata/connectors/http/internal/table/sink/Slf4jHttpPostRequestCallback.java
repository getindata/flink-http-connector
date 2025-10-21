package com.getindata.connectors.http.internal.table.sink;

import java.net.http.HttpResponse;
import java.util.Map;

import lombok.extern.slf4j.Slf4j;

import com.getindata.connectors.http.HttpPostRequestCallback;
import com.getindata.connectors.http.internal.sink.httpclient.HttpRequest;

/**
 * A {@link HttpPostRequestCallback} that logs pairs of request and response as <i>INFO</i> level
 * logs using <i>Slf4j</i>. As the request/response body or header might contain sensitive information,
 * we do not log those values.
 *
 * <p>Serving as a default implementation of {@link HttpPostRequestCallback} for
 * the {@link HttpDynamicSink}.
 */
@Slf4j
public class Slf4jHttpPostRequestCallback implements HttpPostRequestCallback<HttpRequest> {

    @Override
    public void call(
        HttpResponse<String> response,
        HttpRequest requestEntry,
        String endpointUrl,
        Map<String, String> headerMap) {

        // Uncomment if you want to see the requestBody in the log
        //String requestBody = requestEntry.getElements().stream()
        //    .map(element -> new String(element, StandardCharsets.UTF_8))
        //    .collect(Collectors.joining());

        if (response == null) {
            log.info(
                "Got response for a request.\n  Request:\n    " +
                "Method: {}\n   Response: null",
                requestEntry.getMethod()
            );
        } else {
            log.info(
                "Got response for a request.\n  Request:\n    " +
                "Method: {}\n  Response status code: {}\n ",
                requestEntry.method,
                response.statusCode()
            );
        }
    }
}
