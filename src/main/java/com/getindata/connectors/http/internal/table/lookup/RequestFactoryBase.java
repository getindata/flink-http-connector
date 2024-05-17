package com.getindata.connectors.http.internal.table.lookup;

import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpRequest.Builder;
import java.time.Duration;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.FlinkRuntimeException;
import org.slf4j.Logger;

import com.getindata.connectors.http.LookupQueryCreator;
import com.getindata.connectors.http.internal.HeaderPreprocessor;
import com.getindata.connectors.http.internal.auth.OidcAccessTokenManager;
import com.getindata.connectors.http.internal.config.HttpConnectorConfigConstants;
import com.getindata.connectors.http.internal.utils.HttpHeaderUtils;
import static com.getindata.connectors.http.internal.table.lookup.HttpLookupConnectorOptions.*;

/**
 * Base class for {@link HttpRequest} factories.
 */
public abstract class RequestFactoryBase implements HttpRequestFactory{

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
    private final HttpLookupConfig options;

    public RequestFactoryBase(
            LookupQueryCreator lookupQueryCreator,
            HeaderPreprocessor headerPreprocessor,
            HttpLookupConfig options) {

        this.baseUrl = options.getUrl();
        this.lookupQueryCreator = lookupQueryCreator;
        this.options = options;

        var headerMap = HttpHeaderUtils
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

        Builder requestBuilder = setUpRequestMethod(lookupQueryInfo);

        if (headersAndValues.length != 0) {
            requestBuilder.headers(headersAndValues);
        }
        Optional<String> oidcAuthURL = this.options.getReadableConfig()
                .getOptional(SOURCE_LOOKUP_OIDC_AUTH_TOKEN_ENDPOINT_URL);

        if (!oidcAuthURL.isEmpty()) {
            Optional<String> oidcTokenRequest = this.options.getReadableConfig()
                    .getOptional(SOURCE_LOOKUP_OIDC_AUTH_TOKEN_REQUEST);

            Optional<Duration> oidcExpiryReduction = this.options.getReadableConfig()
                    .getOptional(SOURCE_LOOKUP_OIDC_AUTH_TOKEN_EXPIRY_REDUCTION);

            addAccessTokenToRequest(requestBuilder, oidcAuthURL,
                    oidcTokenRequest, oidcExpiryReduction);
        }
        return new HttpLookupSourceRequestEntry(requestBuilder.build(), lookupQueryInfo);
    }

    /**
     * Add the access token to the request using OidcAuth authenticate
     * method that gives us a valid access token.
     * @param requestBuilder request build to add the header to
     * @param oidcAuthURL OIDC token endpoint
     * @param oidcTokenRequest OIDC Token Request
     * @param oidcExpiryReduction OIDC token expiry reduction
     */
    void addAccessTokenToRequest(HttpRequest.Builder requestBuilder,
                                 Optional<String> oidcAuthURL,
                                 Optional<String> oidcTokenRequest,
                                 Optional<Duration> oidcExpiryReduction
    ) {
        OidcAccessTokenManager auth = new OidcAccessTokenManager(
                HttpClient.newBuilder().build(),
                oidcTokenRequest.get(),
                oidcAuthURL.get(),
                oidcExpiryReduction.get()
        );
        // apply the OIDC authentication by adding the correct header.
        requestBuilder.header("Authorization", "BEARER " + auth.authenticate());
    }

    protected abstract Logger getLogger();

    /**
     * Method for preparing {@link HttpRequest.Builder} for concrete REST method.
     * @param lookupQuery lookup query used for request query parameters or body.
     * @return {@link HttpRequest.Builder} for given lookupQuery.
     */
    protected abstract Builder setUpRequestMethod(LookupQueryInfo lookupQuery);

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
