package com.getindata.connectors.http.internal.sink.httpclient.status;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StringUtils;

import com.getindata.connectors.http.internal.config.HttpConnectorConfigConstants;

/**
 * An implementation of {@link HttpStatusCodeChecker} that checks Http Status code against
 * white list, concrete value or {@link HttpType}
 */
public class ComposeHttpStatusCodeChecker implements HttpStatusCodeChecker {

    private static final Set<TypeStatusCodeChecker> DEFAULT_ERROR_CODES =
        Set.of(
            new TypeStatusCodeChecker(HttpType.CLIENT_ERROR),
            new TypeStatusCodeChecker(HttpType.SERVER_ERROR)
        );

    private static final int MIN_HTTP_ERROR_CODE = 100;

    /**
     * Set of {@link HttpStatusCodeChecker} for white listed status codes.
     */
    private final Set<WhiteListHttpStatusCodeChecker> excludedCodes;

    /**
     * Set of {@link HttpStatusCodeChecker} that check status code againts value match or {@link
     * HttpType} match.
     */
    private final Set<HttpStatusCodeChecker> errorCodes = new HashSet<>();

    public ComposeHttpStatusCodeChecker(Properties properties) {
        excludedCodes = prepareWhiteList(properties);
        prepareErrorCodes(properties);
    }

    /**
     * Checks whether given status code is considered as a error code.
     * This implementation checks if status code matches any single value mask like "404"
     * or http type mask such as "4XX". Code that matches one of those masks and is not on a
     * white list will be considered as error code.
     * @param statusCode http status code to assess.
     * @return true if status code is considered as error or false if not.
     */
    public boolean isErrorCode(int statusCode) {

        Preconditions.checkArgument(
            statusCode >= MIN_HTTP_ERROR_CODE,
            String.format(
                "Provided invalid Http status code %s,"
                    + " status code should be equal or bigger than 100.",
                statusCode)
        );

        boolean isWhiteListed = excludedCodes.stream()
            .anyMatch(check -> check.isWhiteListed(statusCode));

        if (isWhiteListed) {
            return false;
        } else {
            return errorCodes.stream()
                .anyMatch(httpStatusCodeChecker -> httpStatusCodeChecker.isErrorCode(statusCode));
        }
    }

    private void prepareErrorCodes(Properties properties) {
        String sErrorCodes =
            properties.getProperty(HttpConnectorConfigConstants.HTTP_ERROR_CODES_LIST, "");

        if (StringUtils.isNullOrWhitespaceOnly(sErrorCodes)) {
            errorCodes.addAll(DEFAULT_ERROR_CODES);
        } else {
            String[] splitCodes = sErrorCodes.split(HttpConnectorConfigConstants.ERROR_CODE_DELIM);

            if (splitCodes.length == 0) {
                errorCodes.addAll(DEFAULT_ERROR_CODES);
            } else {
                prepareErrorCodes(splitCodes);
            }
        }
    }

    /**
     * Process given array of status codes and assign them to
     * {@link SingleValueHttpStatusCodeChecker} for full codes such as 100, 404 etc. or to
     * {@link TypeStatusCodeChecker} for codes that were constructed with "XX" mask
     */
    private void prepareErrorCodes(String[] statusCodes) {
        for (String sCode : statusCodes) {
            if (!StringUtils.isNullOrWhitespaceOnly(sCode)) {
                String trimCode = sCode.toUpperCase().trim();
                boolean isTypeCode = isTypeCode(trimCode);
                if (isTypeCode) {
                    int code = Integer.parseInt(trimCode.replace("X", ""));
                    errorCodes.add(new TypeStatusCodeChecker(HttpType.getByCode(code)));
                } else {
                    errorCodes.add(
                        new SingleValueHttpStatusCodeChecker(Integer.parseInt(trimCode))
                    );
                }
            }
        }
    }

    private Set<WhiteListHttpStatusCodeChecker> prepareWhiteList(Properties properties) {
        return Arrays.stream(
                properties.getProperty(HttpConnectorConfigConstants.HTTP_ERROR_CODE_WHITE_LIST, "")
                    .split(HttpConnectorConfigConstants.ERROR_CODE_DELIM))
            .filter(sCode -> !StringUtils.isNullOrWhitespaceOnly(sCode))
            .map(String::trim)
            .mapToInt(Integer::parseInt)
            .mapToObj(WhiteListHttpStatusCodeChecker::new)
            .collect(Collectors.toSet());
    }

    boolean isTypeCode(final String code) {
        final String substring = "XX";
        final int i = code.indexOf(substring);
        return i >= 1 && i == code.lastIndexOf(substring);
    }
}
