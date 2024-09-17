package com.getindata.connectors.http.internal.table.lookup;

import java.util.Arrays;
import java.util.Map;

import okhttp3.Headers;
import okhttp3.Request;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.FlinkRuntimeException;
import org.slf4j.Logger;

import com.getindata.connectors.http.LookupQueryCreator;
import com.getindata.connectors.http.internal.HeaderPreprocessor;
import com.getindata.connectors.http.internal.config.HttpConnectorConfigConstants;
import com.getindata.connectors.http.internal.utils.HttpHeaderUtils;

/**
 * Base class for {@link HttpRequest} factories.
 */
public abstract class RequestFactoryBase implements HttpRequestFactory {

    public static final String DEFAULT_REQUEST_TIMEOUT_SECONDS = "30";

    /**
     * Base url used for {@link HttpRequest} for example "http://localhost:8080"
     */
    protected final String baseUrl;

    protected final LookupQueryCreator lookupQueryCreator;

    protected final int httpRequestTimeOutSeconds;

    /**
     * HTTP headers that should be used for {@link HttpRequest} created by factory.
     */
    private final String[] headersAndValues;

    public RequestFactoryBase(
            LookupQueryCreator lookupQueryCreator,
            HeaderPreprocessor headerPreprocessor,
            HttpLookupConfig options) {

        this.baseUrl = options.getUrl();
        this.lookupQueryCreator = lookupQueryCreator;

        Map<String, String> headerMap = HttpHeaderUtils
            .prepareHeaderMap(
                HttpConnectorConfigConstants.LOOKUP_SOURCE_HEADER_PREFIX,
                options.getProperties(),
                headerPreprocessor
            );

        this.headersAndValues = HttpHeaderUtils.toHeaderAndValueArray(headerMap);
        this.httpRequestTimeOutSeconds = Integer.parseInt(
            options.getProperties().getProperty(
                HttpConnectorConfigConstants.LOOKUP_HTTP_TIMEOUT_SECONDS,
                DEFAULT_REQUEST_TIMEOUT_SECONDS
            )
        );
    }

    @Override
    public HttpLookupSourceRequestEntry buildLookupRequest(RowData lookupRow) {

        LookupQueryInfo lookupQueryInfo = lookupQueryCreator.createLookupQuery(lookupRow);
        getLogger().debug("Created Http lookup query: " + lookupQueryInfo);

        Request.Builder requestBuilder = setUpRequestMethod(lookupQueryInfo);

        if (headersAndValues.length != 0) {
            requestBuilder.headers(Headers.of(headersAndValues));
        }

        return new HttpLookupSourceRequestEntry(requestBuilder.build(), lookupQueryInfo);
    }

    protected abstract Logger getLogger();

    /**
     * Method for preparing {@link HttpRequest.Builder} for concrete REST method.
     * @param lookupQuery lookup query used for request query parameters or body.
     * @return {@link HttpRequest.Builder} for given lookupQuery.
     */
    protected abstract Request.Builder setUpRequestMethod(LookupQueryInfo lookupQuery);

    protected static StringBuilder resolvePathParameters(LookupQueryInfo lookupQueryInfo,
                                                         StringBuilder resolvedUrl) {
        if (lookupQueryInfo.hasPathBasedUrlParameters()) {
            for (Map.Entry<String, String> entry :
                    lookupQueryInfo.getPathBasedUrlParameters().entrySet()) {
                String pathParam = "{" + entry.getKey() + "}";
                int startIndex = resolvedUrl.indexOf(pathParam);
                if (startIndex == -1) {
                    throw new FlinkRuntimeException(
                            "Unexpected error while parsing the URL for path parameters.");
                }
                int endIndex = startIndex + pathParam.length();
                resolvedUrl = resolvedUrl.replace(startIndex, endIndex, entry.getValue());
            }
        }
        return resolvedUrl;
    }

    @VisibleForTesting
    String[] getHeadersAndValues() {
        return Arrays.copyOf(headersAndValues, headersAndValues.length);
    }
}
