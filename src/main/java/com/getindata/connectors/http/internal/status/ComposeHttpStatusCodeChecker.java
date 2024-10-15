package com.getindata.connectors.http.internal.status;

import java.util.Arrays;
import java.util.Optional;
import java.util.Properties;
import java.util.function.Predicate;

import lombok.AccessLevel;
import lombok.Builder;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import org.apache.flink.util.Preconditions;
import static org.apache.flink.util.StringUtils.isNullOrWhitespaceOnly;

import com.getindata.connectors.http.internal.config.HttpConnectorConfigConstants;

/**
 * An implementation of {@link HttpStatusCodeChecker} that checks Http Status code against
 * white list, concrete value or {@link HttpResponseCodeType}.
 */
public class ComposeHttpStatusCodeChecker implements HttpStatusCodeChecker {

    private static final int MIN_HTTP_STATUS_CODE = 100;
    private static final int MAX_HTTP_STATUS_CODE = 599;

    private static final Predicate<Integer> DEFAULT_ERROR_CODES =
        new TypeStatusCodeCheckerPredicate(HttpResponseCodeType.CLIENT_ERROR);
    private static final Predicate<Integer> DEFAULT_RETRYABLE_ERROR_CODES =
        new TypeStatusCodeCheckerPredicate(HttpResponseCodeType.SERVER_ERROR);
    private static final Predicate<Integer> DEFAULT_DEPRECATED_ERROR_CODES =
        DEFAULT_ERROR_CODES.or(DEFAULT_RETRYABLE_ERROR_CODES);

    private final Predicate<Integer> retryableErrorStatusCodes;
    private final Predicate<Integer> notRetryableErrorStatusCodes;

    public ComposeHttpStatusCodeChecker(ComposeHttpStatusCodeCheckerConfig config) {
        // Handle deprecated configuration for backward compatibility.
        if (areDeprecatedPropertiesUsed(config)) {
            notRetryableErrorStatusCodes = buildPredicate(config, config.getDeprecatedCodePrefix(),
                config.getDeprecatedErrorWhiteListPrefix(), DEFAULT_DEPRECATED_ERROR_CODES);
            retryableErrorStatusCodes = integer -> false;
        } else {
            retryableErrorStatusCodes = buildPredicate(config, config.getRetryableCodePrefix(),
                config.getRetryableWhiteListPrefix(), DEFAULT_RETRYABLE_ERROR_CODES);
            notRetryableErrorStatusCodes = buildPredicate(config, config.getErrorCodePrefix(),
                config.getErrorWhiteListPrefix(), DEFAULT_ERROR_CODES);
        }
    }

    private boolean areDeprecatedPropertiesUsed(ComposeHttpStatusCodeCheckerConfig config) {
        boolean whiteListDefined =
            !isNullOrWhitespaceOnly(config.getDeprecatedErrorWhiteListPrefix());
        boolean codeListDefined = !isNullOrWhitespaceOnly(config.getDeprecatedCodePrefix());

        return (whiteListDefined && !isNullOrWhitespaceOnly(
            config.getProperties().getProperty(config.getDeprecatedErrorWhiteListPrefix())))
            || (codeListDefined && !isNullOrWhitespaceOnly(
            config.getProperties().getProperty(config.getDeprecatedCodePrefix())));
    }

    private Predicate<Integer> buildPredicate(
        ComposeHttpStatusCodeCheckerConfig config,
        String errorCodePrefix,
        String whiteListPrefix,
        Predicate<Integer> defaultErrorCodes) {
        Properties properties = config.getProperties();

        String errorCodes =
            errorCodePrefix == null ? "" : properties.getProperty(errorCodePrefix, "");
        String whitelistCodes =
            whiteListPrefix == null ? "" : properties.getProperty(whiteListPrefix, "");

        Predicate<Integer> errorPredicate =
            prepareErrorCodes(errorCodes).orElse(defaultErrorCodes);
        Predicate<Integer> whitelistPredicate =
            prepareErrorCodes(whitelistCodes).orElse(i -> false);

        return errorPredicate.and(Predicate.not(whitelistPredicate));
    }

    /**
     * Process given string containing comma-separated list of status codes and assign them to
     * {@link SingleValueHttpStatusCodeCheckerPredicate} for full codes such as 100, 404 etc. or to
     * {@link TypeStatusCodeCheckerPredicate} for codes that were constructed with "XX" mask.
     * In the end, all conditions are reduced to a single predicate.
     */
    private Optional<Predicate<Integer>> prepareErrorCodes(String statusCodesStr) {
        return Arrays.stream(statusCodesStr.split(HttpConnectorConfigConstants.PROP_DELIM))
            .filter(code -> !isNullOrWhitespaceOnly(code))
            .map(code -> code.toUpperCase().trim())
            .map(codeStr -> {
                Preconditions.checkArgument(
                    codeStr.length() == 3,
                    "Status code should contain three characters. Provided [%s]",
                    codeStr);

                // at this point we have trim, upper case 3 character status code.
                if (isTypeCode(codeStr)) {
                    int code = Integer.parseInt(codeStr.replace("X", ""));
                    return new TypeStatusCodeCheckerPredicate(
                        HttpResponseCodeType.getByCode(code));
                } else {
                    return new SingleValueHttpStatusCodeCheckerPredicate(
                        Integer.parseInt(codeStr));
                }
            })
            .reduce(Predicate::or);
    }

    /**
     * This method checks if "code" param matches "digit + XX" mask. This method expects that
     * provided string will be 3 elements long, trim and upper case.
     *
     * @param code to check if it contains XX on second ant third position. Parameter is expected to
     *             be 3 characters long, trim and uppercase.
     * @return true if string matches "anything + XX" and false if not.
     */
    private boolean isTypeCode(final String code) {
        return code.charAt(1) == 'X' && code.charAt(2) == 'X';
    }

    /**
     * Checks whether given status code is considered as an error code.
     * This implementation checks if status code matches any single value mask like "404"
     * or http type mask such as "4XX". Code that matches one of those masks and is not on a
     * white list will be considered as error code.
     *
     * @param statusCode http status code to assess.
     * @return <code>SUCCESS</code> if statusCode is considered as success,
     * <code>FAILURE_RETRYABLE</code> if the status code indicates transient error,
     * otherwise <code>FAILURE_NON_RETRYABLE</code>.
     */
    @Override
    public HttpResponseStatus checkStatus(int statusCode) {
        Preconditions.checkArgument(
            statusCode >= MIN_HTTP_STATUS_CODE && statusCode <= MAX_HTTP_STATUS_CODE,
            String.format(
                "Provided invalid Http status code %s,"
                    + " status code should be equal or bigger than %d."
                    + " and equal or lower than %d.",
                statusCode,
                MIN_HTTP_STATUS_CODE,
                MAX_HTTP_STATUS_CODE)
        );

        if (notRetryableErrorStatusCodes.test(statusCode)) {
            return HttpResponseStatus.FAILURE_NOT_RETRYABLE;
        } else if (retryableErrorStatusCodes.test(statusCode)) {
            return HttpResponseStatus.FAILURE_RETRYABLE;
        } else {
            return HttpResponseStatus.SUCCESS;
        }
    }

    @Data
    @Builder
    @RequiredArgsConstructor(access = AccessLevel.PRIVATE)
    public static class ComposeHttpStatusCodeCheckerConfig {

        private final String deprecatedErrorWhiteListPrefix;

        private final String deprecatedCodePrefix;

        private final String errorWhiteListPrefix;

        private final String errorCodePrefix;

        private final String retryableWhiteListPrefix;

        private final String retryableCodePrefix;

        private final Properties properties;
    }
}
