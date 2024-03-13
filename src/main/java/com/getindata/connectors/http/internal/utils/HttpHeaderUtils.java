package com.getindata.connectors.http.internal.utils;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.stream.Stream;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

import com.getindata.connectors.http.internal.BasicAuthHeaderValuePreprocessor;
import com.getindata.connectors.http.internal.ComposeHeaderPreprocessor;
import com.getindata.connectors.http.internal.HeaderPreprocessor;

@NoArgsConstructor(access = AccessLevel.NONE)
public final class HttpHeaderUtils {

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

            String headerName = ConfigUtils.extractPropertyLastElement(propertyName);
            headerMap.put(
                headerName,
                headerPreprocessor.preprocessValueForHeader(headerName, headerValue)
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

    public static HeaderPreprocessor createDefaultHeaderPreprocessor() {
        return createDefaultHeaderPreprocessor(false);
    }

    public static HeaderPreprocessor createDefaultHeaderPreprocessor(boolean useRawAuthHeader) {
        return new ComposeHeaderPreprocessor(
            Collections.singletonMap(
                "Authorization", new BasicAuthHeaderValuePreprocessor(useRawAuthHeader))
        );
    }
}
