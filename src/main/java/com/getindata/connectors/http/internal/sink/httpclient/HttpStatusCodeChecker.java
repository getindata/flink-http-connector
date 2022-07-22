package com.getindata.connectors.http.internal.sink.httpclient;

import java.util.Arrays;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StringUtils;

import com.getindata.connectors.http.internal.config.HttpConnectorConfigConstants;

public class HttpStatusCodeChecker {

    private static final String DEFAULT_ERROR_CODES = String.join(
        HttpConnectorConfigConstants.ERROR_CODE_DELIM,
        "400", "500");

    private final Set<Integer> excludedCodes;

    private final Set<String> errorCodes;

    public HttpStatusCodeChecker(Properties properties) {

        excludedCodes = Arrays.stream(
                properties.getProperty(HttpConnectorConfigConstants.HTTP_ERROR_CODE_WHITE_LIST, "")
                    .split(HttpConnectorConfigConstants.ERROR_CODE_DELIM))
            .filter(sCode -> !StringUtils.isNullOrWhitespaceOnly(sCode))
            .map(String::trim)
            .mapToInt(Integer::parseInt)
            .boxed()
            .collect(Collectors.toSet());

        errorCodes = Arrays.stream(
                properties.getProperty(
                        HttpConnectorConfigConstants.HTTP_ERROR_CODES_LIST,
                        DEFAULT_ERROR_CODES)
                    .split(HttpConnectorConfigConstants.ERROR_CODE_DELIM))
            .filter(sCode -> !StringUtils.isNullOrWhitespaceOnly(sCode))
            .map(String::trim)
            .map(String::toUpperCase)
            .collect(Collectors.toSet());
    }

    public boolean isErrorCode(int statusCode) {

        Preconditions.checkArgument(
            statusCode >= 100,
            String.format(
                "Provided invalid Http status code %s,"
                    + " status code should be equal or bigger than 100.",
                statusCode)
        );

        if (excludedCodes.contains(statusCode)) {
            return false;
        }

        return errorCodes.contains(statusCode / 100 + "XX")
            || errorCodes.contains(String.valueOf(statusCode));
    }

    public boolean isNotErrorCode(int statusCode) {
        return !isErrorCode(statusCode);
    }

}
