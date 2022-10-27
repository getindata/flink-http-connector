package com.getindata.connectors.http.internal.table.lookup;

import java.net.http.HttpRequest;
import java.net.http.HttpRequest.Builder;
import java.util.Arrays;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.table.data.RowData;
import org.slf4j.Logger;

import com.getindata.connectors.http.LookupQueryCreator;
import com.getindata.connectors.http.internal.HeaderPreprocessor;
import com.getindata.connectors.http.internal.config.HttpConnectorConfigConstants;
import com.getindata.connectors.http.internal.utils.HttpHeaderUtils;

/**
 * Base class for {@link HttpRequest} factories.
 */
public abstract class RequestFactoryBase implements HttpRequestFactory {

    /**
     * Base url used for {@link HttpRequest} for example "http://localhost:8080"
     */
    protected final String baseUrl;

    protected final LookupQueryCreator lookupQueryCreator;

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

        var headerMap = HttpHeaderUtils
            .prepareHeaderMap(
                HttpConnectorConfigConstants.LOOKUP_SOURCE_HEADER_PREFIX,
                options.getProperties(),
                headerPreprocessor
            );

        this.headersAndValues = HttpHeaderUtils.toHeaderAndValueArray(headerMap);
    }

    @Override
    public HttpRequest buildLookupRequest(RowData lookupRow) {

        String lookupQuery = lookupQueryCreator.createLookupQuery(lookupRow);
        getLogger().debug("Created Http lookup query: " + lookupQuery);

        Builder requestBuilder = setUpRequestMethod(lookupQuery);

        if (headersAndValues.length != 0) {
            requestBuilder.headers(headersAndValues);
        }

        return requestBuilder.build();
    }

    protected abstract Logger getLogger();

    /**
     * Method for preparing {@link HttpRequest.Builder} for concrete REST method.
     * @param lookupQuery lookup query used for request query parameters or body.
     * @return {@link HttpRequest.Builder} for given lookupQuery.
     */
    protected abstract Builder setUpRequestMethod(String lookupQuery);

    @VisibleForTesting
    String[] getHeadersAndValues() {
        return Arrays.copyOf(headersAndValues, headersAndValues.length);
    }
}
