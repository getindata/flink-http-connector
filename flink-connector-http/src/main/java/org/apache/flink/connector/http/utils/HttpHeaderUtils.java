/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.http.utils;

import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.http.preprocessor.BasicAuthHeaderValuePreprocessor;
import org.apache.flink.connector.http.preprocessor.ComposeHeaderPreprocessor;
import org.apache.flink.connector.http.preprocessor.HeaderPreprocessor;
import org.apache.flink.connector.http.preprocessor.OIDCAuthHeaderValuePreprocessor;
import org.apache.flink.connector.http.table.lookup.HttpLookupConnectorOptions;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.Stream;

import static org.apache.flink.connector.http.table.lookup.HttpLookupConnectorOptions.SOURCE_LOOKUP_OIDC_AUTH_TOKEN_ENDPOINT_URL;
import static org.apache.flink.connector.http.table.lookup.HttpLookupConnectorOptions.SOURCE_LOOKUP_OIDC_AUTH_TOKEN_EXPIRY_REDUCTION;
import static org.apache.flink.connector.http.table.lookup.HttpLookupConnectorOptions.SOURCE_LOOKUP_OIDC_AUTH_TOKEN_REQUEST;

/** Http header utils. */
@UtilityClass
@NoArgsConstructor(access = AccessLevel.NONE)
@Slf4j
public final class HttpHeaderUtils {

    public static final String AUTHORIZATION = "Authorization";

    public static Map<String, String> prepareHeaderMap(
            String headerKeyPrefix, Properties properties, HeaderPreprocessor headerPreprocessor) {

        //  at this stage headerMap keys are full property paths not only header names.
        Map<String, String> propertyHeaderMap =
                ConfigUtils.propertiesToMap(properties, headerKeyPrefix, String.class);

        // Map with keys pointing to the headerName.
        Map<String, String> headerMap = new HashMap<>();

        for (Entry<String, String> headerAndValue : propertyHeaderMap.entrySet()) {
            String propertyName = headerAndValue.getKey();
            String headerValue = headerAndValue.getValue();
            log.info(
                    "prepareHeaderMap propertyName=" + propertyName + ",headerValue" + headerValue);
            String headerName = ConfigUtils.extractPropertyLastElement(propertyName);
            String preProcessedHeader =
                    headerPreprocessor.preprocessValueForHeader(headerName, headerValue);
            log.info("prepareHeaderMap preProcessedHeader=" + preProcessedHeader);
            headerMap.put(headerName, preProcessedHeader);
        }
        return headerMap;
    }

    /**
     * Flat map a given Map of header name and header value map to an array containing both header
     * names and values. For example, header map of
     *
     * <pre>{@code
     * Map.of(
     * header1, val1,
     * header2, val2
     * )
     * }</pre>
     *
     * <p>will be converter to an array of:
     *
     * <pre>{@code
     * String[] headers = {"header1", "val1", "header2", "val2"};
     * }</pre>
     *
     * @param headerMap mapping of header names to header values
     * @return an array containing both header names and values
     */
    public static String[] toHeaderAndValueArray(Map<String, String> headerMap) {
        return headerMap.entrySet().stream()
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
                        AUTHORIZATION, new BasicAuthHeaderValuePreprocessor(useRawAuthHeader)));
    }

    public static HeaderPreprocessor createOIDCAuthorizationHeaderPreprocessor(
            String oidcAuthURL, String oidcTokenRequest, Optional<Duration> oidcExpiryReduction) {
        return new ComposeHeaderPreprocessor(
                Collections.singletonMap(
                        AUTHORIZATION,
                        new OIDCAuthHeaderValuePreprocessor(
                                oidcAuthURL, oidcTokenRequest, oidcExpiryReduction)));
    }

    public static HeaderPreprocessor createHeaderPreprocessor(ReadableConfig readableConfig) {
        boolean useRawAuthHeader =
                readableConfig.get(HttpLookupConnectorOptions.USE_RAW_AUTH_HEADER);
        HeaderPreprocessor headerPreprocessor =
                HttpHeaderUtils.createBasicAuthorizationHeaderPreprocessor(useRawAuthHeader);
        log.info("created HeaderPreprocessor for basic useRawAuthHeader");
        return headerPreprocessor;
    }

    public static HeaderPreprocessor createOIDCHeaderPreprocessor(ReadableConfig readableConfig) {
        HeaderPreprocessor headerPreprocessor = null;
        Optional<String> oidcAuthURL =
                readableConfig.getOptional(SOURCE_LOOKUP_OIDC_AUTH_TOKEN_ENDPOINT_URL);

        if (oidcAuthURL.isPresent()) {
            Optional<String> oidcTokenRequest =
                    readableConfig.getOptional(SOURCE_LOOKUP_OIDC_AUTH_TOKEN_REQUEST);

            Optional<Duration> oidcExpiryReduction =
                    readableConfig.getOptional(SOURCE_LOOKUP_OIDC_AUTH_TOKEN_EXPIRY_REDUCTION);
            headerPreprocessor =
                    HttpHeaderUtils.createOIDCAuthorizationHeaderPreprocessor(
                            oidcAuthURL.get(), oidcTokenRequest.get(), oidcExpiryReduction);
            log.info("created OIDC HeaderPreprocessor");
        }
        return headerPreprocessor;
    }
}
