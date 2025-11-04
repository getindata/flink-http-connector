package com.getindata.connectors.http.internal.table.lookup;

import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpRequest;

import org.junit.jupiter.api.Test;

class Slf4JHttpLookupPostRequestCallbackTest {
    @Test
    public void testNullResponseDoesNotError() throws URISyntaxException {
        HttpRequest httpRequest = HttpRequest.newBuilder()
            .method("GET", HttpRequest.BodyPublishers.ofString("foo"))
            .uri(new URI("http://testing123")).build();
        HttpLookupSourceRequestEntry requestEntry =
            new HttpLookupSourceRequestEntry(httpRequest, new LookupQueryInfo(""));
        Slf4JHttpLookupPostRequestCallback slf4JHttpLookupPostRequestCallback =
            new Slf4JHttpLookupPostRequestCallback();
        slf4JHttpLookupPostRequestCallback.call(null, requestEntry, "aaa", null);
    }
}
