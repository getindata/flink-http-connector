package com.getindata.connectors.http.internal.utils;

import java.time.Duration;
import java.util.*;
import java.util.Map.Entry;
import java.util.stream.Stream;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.ReadableConfig;

import com.getindata.connectors.http.internal.BasicAuthHeaderValuePreprocessor;
import com.getindata.connectors.http.internal.ComposeHeaderPreprocessor;
import com.getindata.connectors.http.internal.HeaderPreprocessor;
import com.getindata.connectors.http.internal.OIDCAuthHeaderValuePreprocessor;
import com.getindata.connectors.http.internal.table.lookup.HttpLookupConnectorOptions;
import static com.getindata.connectors.http.internal.table.lookup.HttpLookupConnectorOptions.*;



@NoArgsConstructor(access = AccessLevel.NONE)
@Slf4j
public final class HttpHeaderUtils {

    public static final String AUTHORIZATION = "Authorization";

    public static Map<String, String> prepareHeaderMap(
            String headerKeyPrefix,
            Properties properties,
            HeaderPreprocessor headerPreprocessor) {

        //  at this stage headerMap keys are full property paths not only header names.
        Map<String, String> propertyHeaderMap =
            ConfigUtils.propertiesToMap(properties, headerKeyPrefix, String.class);

        // Map with keys pointing to the headerName.
        Map<String, String> headerMap = new HashMap<>();

        for (Entry<String, String> headerAndValue : propertyHeaderMap.entrySet()) {
            String propertyName = headerAndValue.getKey();
            String headerValue = headerAndValue.getValue();
            log.info("prepareHeaderMap propertyName=" + propertyName
                    + ",headerValue" + headerValue);
            String headerName = ConfigUtils.extractPropertyLastElement(propertyName);
            String preProcessedHeader =
                    headerPreprocessor.preprocessValueForHeader(headerName, headerValue);
            log.info("prepareHeaderMap preProcessedHeader="
                    + preProcessedHeader);
            headerMap.put(
                headerName,
                preProcessedHeader
            );
        }
        return headerMap;
    }

    /**
     * Flat map a given Map of header name and header value map to an array containing both header
     * names and values. For example, header map of
     * <pre>{@code
     *     Map.of(
     *     header1, val1,
     *     header2, val2
     *     )
     * }</pre>
     * will be converter to an array of:
     * <pre>{@code
     *      String[] headers = {"header1", "val1", "header2", "val2"};
     * }</pre>
     *
     * @param headerMap mapping of header names to header values
     * @return an array containing both header names and values
     */
    public static String[] toHeaderAndValueArray(Map<String, String> headerMap) {
        return headerMap
            .entrySet()
            .stream()
            .flatMap(entry -> Stream.of(entry.getKey(), entry.getValue()))
            .toArray(String[]::new);
    }

    public static HeaderPreprocessor createBasicAuthorizationHeaderPreprocessor() {
        return createBasicAuthorizationHeaderPreprocessor(false);
    }

    public static HeaderPreprocessor createBasicAuthorizationHeaderPreprocessor(
            boolean useRawAuthHeader) {
        return new ComposeHeaderPreprocessor(
            Collections.singletonMap(
                AUTHORIZATION, new BasicAuthHeaderValuePreprocessor(useRawAuthHeader))
        );
    }

    public static HeaderPreprocessor createOIDCAuthorizationHeaderPreprocessor(
            String oidcAuthURL,
            String oidcTokenRequest,
            Optional<Duration> oidcExpiryReduction
    ) {
        return new ComposeHeaderPreprocessor(
                Collections.singletonMap(
                        AUTHORIZATION, new OIDCAuthHeaderValuePreprocessor(oidcAuthURL,
                                oidcTokenRequest, oidcExpiryReduction))
        );
    }

    public static HeaderPreprocessor createHeaderPreprocessor(ReadableConfig readableConfig) {
        HeaderPreprocessor headerPreprocessor;
        Optional<String> oidcAuthURL = readableConfig
                .getOptional(SOURCE_LOOKUP_OIDC_AUTH_TOKEN_ENDPOINT_URL);

        if(oidcAuthURL.isPresent()) {
            Optional<String> oidcTokenRequest = readableConfig
                    .getOptional(SOURCE_LOOKUP_OIDC_AUTH_TOKEN_REQUEST);

            Optional<Duration> oidcExpiryReduction = readableConfig
                    .getOptional(SOURCE_LOOKUP_OIDC_AUTH_TOKEN_EXPIRY_REDUCTION);
            headerPreprocessor = HttpHeaderUtils.createOIDCAuthorizationHeaderPreprocessor(
                 oidcAuthURL.get(), oidcTokenRequest.get(), oidcExpiryReduction);
            log.info("created HeaderPreprocessor " + headerPreprocessor
                    + " for OIDC oidcAuthURL=" + oidcAuthURL
                    + ", oidcTokenRequest=" + oidcTokenRequest
                    + ", oidcExpiryReduction=" + oidcExpiryReduction);
        } else {
            boolean useRawAuthHeader =
                    readableConfig.get(HttpLookupConnectorOptions.USE_RAW_AUTH_HEADER);

            headerPreprocessor =
                    HttpHeaderUtils.createBasicAuthorizationHeaderPreprocessor(
                            useRawAuthHeader);
            log.info("created HeaderPreprocessor for basic useRawAuthHeader=" + useRawAuthHeader);
        }
        log.info("returning HeaderPreprocessor " + headerPreprocessor);
        return headerPreprocessor;
    }
}
