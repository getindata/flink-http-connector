package com.getindata.connectors.http.internal.table.lookup;

import java.net.http.HttpRequest;
import java.net.http.HttpRequest.BodyPublishers;
import java.net.http.HttpRequest.Builder;
import java.time.Duration;

import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;

import com.getindata.connectors.http.LookupQueryCreator;
import com.getindata.connectors.http.internal.HeaderPreprocessor;

/**
 * Implementation of {@link HttpRequestFactory} for REST calls that sends their parameters using
 * request body or in the path.
 */
@Slf4j
public class BodyAndPathBasedRequestFactory extends RequestFactoryBase {

    private final String methodName;

    public BodyAndPathBasedRequestFactory(
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
     * @param lookupQueryInfo lookup query info used for request body.
     * @return {@link HttpRequest.Builder} for given lookupQuery.
     */
    @Override
    protected Builder setUpRequestMethod(LookupQueryInfo lookupQueryInfo) {
        return HttpRequest.newBuilder()
            .uri(lookupQueryInfo.getURI())
            .method(methodName, BodyPublishers.ofString(lookupQueryInfo.getLookupQuery()))
            .timeout(Duration.ofSeconds(this.httpRequestTimeOutSeconds));
    }

    @Override
    protected Logger getLogger() {
        return log;
    }

}
