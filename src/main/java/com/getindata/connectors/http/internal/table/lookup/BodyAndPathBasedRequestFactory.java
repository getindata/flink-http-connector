package com.getindata.connectors.http.internal.table.lookup;

import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpRequest;
import java.net.http.HttpRequest.BodyPublishers;
import java.net.http.HttpRequest.Builder;
import java.time.Duration;
import java.util.Map;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.util.FlinkRuntimeException;
import org.slf4j.Logger;

import com.getindata.connectors.http.LookupQueryCreator;
import com.getindata.connectors.http.internal.HeaderPreprocessor;
import com.getindata.connectors.http.internal.utils.uri.URIBuilder;

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

//    URI constructUri(LookupQueryInfo lookupQueryInfo) {
//        StringBuilder resolvedUrl = new StringBuilder(baseUrl);
//        if (lookupQueryInfo.hasBodyBasedUrlQueryParameters()) {
//            resolvedUrl.append(baseUrl.contains("?") ? "&" : "?")
//                       .append(lookupQueryInfo.getBodyBasedUrlQueryParameters());
//        }
//        if (lookupQueryInfo.hasPathBasedUrlParameters()) {
//            for (Map.Entry<String, String> entry :
//                    lookupQueryInfo.getPathBasedUrlParameters().entrySet()) {
//                String pathParam = "{" + entry.getKey() + "}";
//                int startIndex = resolvedUrl.indexOf(pathParam);
//                if (startIndex == -1) {
//                    throw new FlinkRuntimeException(
//                            "Unexpected error while parsing the URL for path parameters.");
//                }
//                int endIndex = startIndex + pathParam.length();
//                resolvedUrl = resolvedUrl.replace(startIndex, endIndex, entry.getValue());
//            }
//        }

//        try {
//            return new URIBuilder(resolvedUrl.toString()).build();
//        } catch (URISyntaxException e) {
//            throw new RuntimeException(e);
//        }
//    }
}
