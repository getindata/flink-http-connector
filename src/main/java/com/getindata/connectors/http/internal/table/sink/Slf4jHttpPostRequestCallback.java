package com.getindata.connectors.http.internal.table.sink;

import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import lombok.extern.slf4j.Slf4j;

import com.getindata.connectors.http.HttpPostRequestCallback;
import com.getindata.connectors.http.internal.sink.HttpSinkRequestEntry;
import com.getindata.connectors.http.internal.utils.ConfigUtils;

/**
 * A {@link HttpPostRequestCallback} that logs pairs of request and response as <i>INFO</i> level
 * logs using <i>Slf4j</i>.
 *
 * <p>Serving as a default implementation of {@link HttpPostRequestCallback} for
 * the {@link HttpDynamicSink}.
 */
@Slf4j
public class Slf4jHttpPostRequestCallback implements HttpPostRequestCallback<HttpSinkRequestEntry> {

    @Override
    public void call(
        HttpResponse<String> response,
        HttpSinkRequestEntry requestEntry,
        String endpointUrl,
        Map<String, String> headerMap) {

        if (response == null) {
            log.info(
                "Got response for a request.\n  Request:\n    " +
                "Method: {}\n    Body: {}\n  Response: null",
                requestEntry.method,
                new String(requestEntry.element, StandardCharsets.UTF_8)
            );
        } else {
            log.info(
                "Got response for a request.\n  Request:\n    " +
                "Method: {}\n    Body: {}\n  Response: {}\n    Body: {}",
                requestEntry.method,
                new String(requestEntry.element, StandardCharsets.UTF_8),
                response,
                response.body().replaceAll(ConfigUtils.UNIVERSAL_NEW_LINE_REGEXP, "")
            );
        }
    }
}
