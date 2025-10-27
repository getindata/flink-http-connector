package com.getindata.connectors.http.internal.table.lookup;

import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Map;

import lombok.extern.slf4j.Slf4j;

import com.getindata.connectors.http.HttpPostRequestCallback;

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

        if (response == null) {
            log.warn("Null Http response for request " + httpRequest.uri().toString());

            log.info(
                "Got response for a request.\n  Request:\n    URL: {}\n    " +
                    "Method: {}\n Params/Body: {}\nResponse: null",
                httpRequest.uri().toString(),
                httpRequest.method(),
                requestEntry.getLookupQueryInfo()
            );
        } else {
            log.info(
                "Got response for a request.\n  Request:\n    URL: {}\n    " +
                    "Method: {}\n Params/Body: {}\nResponse status code: {}\n",
                httpRequest.uri().toString(),
                httpRequest.method(),
                requestEntry.getLookupQueryInfo(),
                response.statusCode()
            );
        }
    }
}
