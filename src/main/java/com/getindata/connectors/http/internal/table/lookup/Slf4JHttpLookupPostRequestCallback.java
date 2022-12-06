package com.getindata.connectors.http.internal.table.lookup;

import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.StringJoiner;

import lombok.extern.slf4j.Slf4j;

import com.getindata.connectors.http.HttpPostRequestCallback;
import com.getindata.connectors.http.internal.utils.ConfigUtils;

/**
 * A {@link HttpPostRequestCallback} that logs pairs of request and response as <i>INFO</i> level
 * logs using <i>Slf4j</i>.
 *
 * <p>Serving as a default implementation of {@link HttpPostRequestCallback} for
 * the {@link HttpLookupTableSource}.
 */
@Slf4j
public class Slf4JHttpLookupPostRequestCallback implements HttpPostRequestCallback<HttpRequest> {

    @Override
    public void call(
            HttpResponse<String> response,
            HttpRequest requestEntry,
            String endpointUrl,
            Map<String, String> headerMap) {

        StringJoiner headers = new StringJoiner(";");
        for (Entry<String, List<String>> reqHeaders : requestEntry.headers().map().entrySet()) {
            StringJoiner values = new StringJoiner(";");
            for (String value : reqHeaders.getValue()) {
                values.add(value);
            }
            String header = reqHeaders.getKey() + ": [" + values + "]";
            headers.add(header);
        }

        if (response == null) {
            log.info(
                "Got response for a request.\n  Request:\n    " +
                    "Method: {}\n    Headers: {}\n    Body: {}\n    Response: null",
                requestEntry.method(), headers, requestEntry
            );
        } else {
            log.info(
                "Got response for a request.\n  Request:\n    " +
                    "Method: {}\n    Headers: {}\n    Body: {}\n    Response: {}\n    Body: {}",
                requestEntry.method(),
                headers,
                requestEntry,
                response,
                response.body().replaceAll(ConfigUtils.UNIVERSAL_NEW_LINE_REGEXP, "")
            );
        }

    }
}
