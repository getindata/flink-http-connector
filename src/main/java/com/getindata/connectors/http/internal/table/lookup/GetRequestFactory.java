package com.getindata.connectors.http.internal.table.lookup;

import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpRequest;
import java.net.http.HttpRequest.Builder;
import java.time.Duration;

import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;

import com.getindata.connectors.http.LookupQueryCreator;
import com.getindata.connectors.http.internal.HeaderPreprocessor;
import com.getindata.connectors.http.internal.utils.uri.URIBuilder;

/**
 * Implementation of {@link HttpRequestFactory} for GET REST calls.
 */
@Slf4j
public class GetRequestFactory extends RequestFactoryBase {

    public GetRequestFactory(
            LookupQueryCreator lookupQueryCreator,
            HeaderPreprocessor headerPreprocessor,
            HttpLookupConfig options) {

        super(lookupQueryCreator, headerPreprocessor, options);
    }

    @Override
    protected Logger getLogger() {
        return log;
    }

    /**
     * Method for preparing {@link HttpRequest.Builder} for REST GET request, where lookupQuery
     * is used as query parameters for example:
     * <pre>
     *     http:localhost:8080/service?id=1
     * </pre>
     * @param lookupQuery lookup query used for request query parameters.
     * @return {@link HttpRequest.Builder} for given GET lookupQuery
     */
    @Override
    protected Builder setUpRequestMethod(String lookupQuery) {
        return HttpRequest.newBuilder()
            .uri(constructGetUri(lookupQuery))
            .GET()
            .timeout(Duration.ofSeconds(this.httpRequestTimeOutSeconds));
    }

    private URI constructGetUri(String lookupQuery) {
        try {
            return new URIBuilder(baseUrl + "?" + lookupQuery).build();
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }
}
