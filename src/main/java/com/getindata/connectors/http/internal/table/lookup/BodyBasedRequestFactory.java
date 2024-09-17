package com.getindata.connectors.http.internal.table.lookup;

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import okhttp3.Request;
import okhttp3.RequestBody;
import org.slf4j.Logger;

import com.getindata.connectors.http.LookupQueryCreator;
import com.getindata.connectors.http.internal.HeaderPreprocessor;
import com.getindata.connectors.http.internal.utils.uri.URIBuilder;

/**
 * Implementation of {@link HttpRequestFactory} for REST calls that sends their parameters using
 * request body or in the path.
 */
@Slf4j
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
     * @param lookupQueryInfo lookup query info used for request body.
     * @return {@link HttpRequest.Builder} for given lookupQuery.
     */
    @Override
    @SneakyThrows
    protected Request.Builder setUpRequestMethod(LookupQueryInfo lookupQueryInfo) {
        return new Request.Builder()
            .url(constructUri(lookupQueryInfo).toURL().toString())
            .method(methodName, RequestBody.create(lookupQueryInfo.getLookupQuery().getBytes(StandardCharsets.UTF_8)));
//            .timeout(Duration.ofSeconds(this.httpRequestTimeOutSeconds));
    }

    @Override
    protected Logger getLogger() {
        return log;
    }

    URI constructUri(LookupQueryInfo lookupQueryInfo) {
        StringBuilder resolvedUrl = new StringBuilder(baseUrl);
        if (lookupQueryInfo.hasBodyBasedUrlQueryParameters()) {
            resolvedUrl.append(baseUrl.contains("?") ? "&" : "?")
                       .append(lookupQueryInfo.getBodyBasedUrlQueryParameters());
        }
        resolvedUrl = resolvePathParameters(lookupQueryInfo, resolvedUrl);

        try {
            return new URIBuilder(resolvedUrl.toString()).build();
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

}
