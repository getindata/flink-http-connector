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
public class Slf4JHttpLookupPostRequestCallback
        implements HttpPostRequestCallback<HttpLookupSourceRequestEntry> {

    @Override
    public void call(
            HttpResponse<String> response,
            HttpLookupSourceRequestEntry requestEntry,
            String endpointUrl,
            Map<String, String> headerMap) {

        HttpRequest httpRequest = requestEntry.getHttpRequest();
        StringJoiner headers = new StringJoiner(";");

        for (Entry<String, List<String>> reqHeaders : httpRequest.headers().map().entrySet()) {
            StringJoiner values = new StringJoiner(";");
            for (String value : reqHeaders.getValue()) {
                values.add(value);
            }
            String header = reqHeaders.getKey() + ": [" + values + "]";
            headers.add(header);
        }

        if (response == null) {
            log.info(
                "Got response for a request.\n  Request:\n    URL: {}\n    " +
                    "Method: {}\n    Headers: {}\n    Params/Body: {}\nResponse: null",
                httpRequest.uri().toString(),
                httpRequest.method(),
                headers,
                requestEntry.getLookupQueryInfo()
            );
        } else {
            log.info(
                "Got response for a request.\n  Request:\n    URL: {}\n    " +
                    "Method: {}\n    Headers: {}\n    Params/Body: {}\nResponse: {}\n    Body: {}",
                httpRequest.uri().toString(),
                httpRequest.method(),
                headers,
                requestEntry.getLookupQueryInfo(),
                response,
                response.body().replaceAll(ConfigUtils.UNIVERSAL_NEW_LINE_REGEXP, "")
            );
        }

    }
}
