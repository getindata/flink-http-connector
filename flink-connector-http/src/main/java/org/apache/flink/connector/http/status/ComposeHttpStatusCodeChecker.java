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

package org.apache.flink.connector.http.status;

import org.apache.flink.connector.http.config.HttpConnectorConfigConstants;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StringUtils;

import lombok.AccessLevel;
import lombok.Builder;
import lombok.Data;
import lombok.RequiredArgsConstructor;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * An implementation of {@link HttpStatusCodeChecker} that checks Http Status code against include
 * list, concrete value or {@link HttpResponseCodeType}.
 */
public class ComposeHttpStatusCodeChecker implements HttpStatusCodeChecker {

    private static final Set<HttpStatusCodeChecker> DEFAULT_ERROR_CODES =
            Set.of(
                    new TypeStatusCodeChecker(HttpResponseCodeType.CLIENT_ERROR),
                    new TypeStatusCodeChecker(HttpResponseCodeType.SERVER_ERROR));

    private static final int MIN_HTTP_STATUS_CODE = 100;

    /** Set of {@link HttpStatusCodeChecker} for include listed status codes. */
    private final Set<IncludeListHttpStatusCodeChecker> includedCodes;

    /**
     * Set of {@link HttpStatusCodeChecker} that check status code againts value match or {@link
     * HttpResponseCodeType} match.
     */
    private final Set<HttpStatusCodeChecker> errorCodes;

    public ComposeHttpStatusCodeChecker(ComposeHttpStatusCodeCheckerConfig config) {
        includedCodes = prepareIncludeList(config);
        errorCodes = prepareErrorCodes(config);
    }

    /**
     * Checks whether given status code is considered as a error code. This implementation checks if
     * status code matches any single value mask like "404" or http type mask such as "4XX". Code
     * that matches one of those masks and is not on an include list will be considered as error
     * code.
     *
     * @param statusCode http status code to assess.
     * @return true if status code is considered as error or false if not.
     */
    public boolean isErrorCode(int statusCode) {

        Preconditions.checkArgument(
                statusCode >= MIN_HTTP_STATUS_CODE,
                String.format(
                        "Provided invalid Http status code %s,"
                                + " status code should be equal or bigger than %d.",
                        statusCode, MIN_HTTP_STATUS_CODE));

        boolean isOnIncludeList =
                includedCodes.stream().anyMatch(check -> check.isOnIncludeList(statusCode));

        return !isOnIncludeList
                && errorCodes.stream()
                        .anyMatch(
                                httpStatusCodeChecker ->
                                        httpStatusCodeChecker.isErrorCode(statusCode));
    }

    private Set<HttpStatusCodeChecker> prepareErrorCodes(
            ComposeHttpStatusCodeCheckerConfig config) {

        Properties properties = config.getProperties();
        String errorCodePrefix = config.getErrorCodePrefix();

        String errorCodes = properties.getProperty(errorCodePrefix, "");

        if (StringUtils.isNullOrWhitespaceOnly(errorCodes)) {
            return DEFAULT_ERROR_CODES;
        } else {
            String[] splitCodes = errorCodes.split(HttpConnectorConfigConstants.PROP_DELIM);
            return prepareErrorCodes(splitCodes);
        }
    }

    /**
     * Process given array of status codes and assign them to {@link
     * SingleValueHttpStatusCodeChecker} for full codes such as 100, 404 etc. or to {@link
     * TypeStatusCodeChecker} for codes that were constructed with "XX" mask
     */
    private Set<HttpStatusCodeChecker> prepareErrorCodes(String[] statusCodes) {

        Set<HttpStatusCodeChecker> errorCodes = new HashSet<>();
        for (String sCode : statusCodes) {
            if (!StringUtils.isNullOrWhitespaceOnly(sCode)) {
                String trimCode = sCode.toUpperCase().trim();
                Preconditions.checkArgument(
                        trimCode.length() == 3,
                        "Status code should contain three characters. Provided [%s]",
                        trimCode);

                // at this point we have trim, upper case 3 character status code.
                if (isTypeCode(trimCode)) {
                    int code = Integer.parseInt(trimCode.replace("X", ""));
                    errorCodes.add(new TypeStatusCodeChecker(HttpResponseCodeType.getByCode(code)));
                } else {
                    errorCodes.add(
                            new SingleValueHttpStatusCodeChecker(Integer.parseInt(trimCode)));
                }
            }
        }
        return (errorCodes.isEmpty()) ? DEFAULT_ERROR_CODES : errorCodes;
    }

    private Set<IncludeListHttpStatusCodeChecker> prepareIncludeList(
            ComposeHttpStatusCodeCheckerConfig config) {

        Properties properties = config.getProperties();
        String includeListPrefix = config.getIncludeListPrefix();

        return Arrays.stream(
                        properties
                                .getProperty(includeListPrefix, "")
                                .split(HttpConnectorConfigConstants.PROP_DELIM))
                .filter(sCode -> !StringUtils.isNullOrWhitespaceOnly(sCode))
                .map(String::trim)
                .mapToInt(Integer::parseInt)
                .mapToObj(IncludeListHttpStatusCodeChecker::new)
                .collect(Collectors.toSet());
    }

    /**
     * This method checks if "code" param matches "digit + XX" mask. This method expects that
     * provided string will be 3 elements long, trim and upper case.
     *
     * @param code to check if it contains XX on second and third position. Parameter is expected to
     *     be 3 characters long, trim and uppercase.
     * @return true if string matches "anything + XX" and false if not.
     */
    private boolean isTypeCode(final String code) {
        return code.charAt(1) == 'X' && code.charAt(2) == 'X';
    }

    /** config. */
    @Data
    @Builder
    @RequiredArgsConstructor(access = AccessLevel.PRIVATE)
    public static class ComposeHttpStatusCodeCheckerConfig {

        private final String includeListPrefix;

        private final String errorCodePrefix;

        private final Properties properties;
    }
}
