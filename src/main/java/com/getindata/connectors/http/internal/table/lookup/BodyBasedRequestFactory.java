package com.getindata.connectors.http.internal.table.lookup;

import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpRequest;
import java.net.http.HttpRequest.BodyPublishers;
import java.net.http.HttpRequest.Builder;
import java.time.Duration;

import com.getindata.connectors.http.LookupQueryCreator;
import com.getindata.connectors.http.internal.HeaderPreprocessor;
import com.getindata.connectors.http.internal.utils.uri.URIBuilder;

/**
 * Implementation of {@link HttpRequestFactory} for REST calls that sends their parameters using
 * request body.
 */
public class BodyBasedRequestFactory extends RequestFactoryBase {

    private final String methodName;

    public BodyBasedRequestFactory(
            String methodName,
            LookupQueryCreator lookupQueryCreator,
            HeaderPreprocessor headerPreprocessor,
            HttpLookupConfig options) {

        super(lookupQueryCreator, headerPreprocessor, options);
        this.methodName = methodName.toUpperCase();
    }

    /**
     * Method for preparing {@link HttpRequest.Builder} for REST request that sends their parameters
     * in request body, for example PUT or POST methods
     *
     * @param lookupQuery lookup query used for request body.
     * @return {@link HttpRequest.Builder} for given lookupQuery.
     */
    @Override
    protected Builder setUpRequestMethod(String lookupQuery) {
        return HttpRequest.newBuilder()
            .uri(constructGetUri())
            .method(methodName, BodyPublishers.ofString(lookupQuery))
            .timeout(Duration.ofMinutes(2));
    }

    private URI constructGetUri() {
        try {
            return new URIBuilder(baseUrl).build();
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }
}
