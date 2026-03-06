package com.getindata.connectors.http.internal.status;

import java.net.http.HttpResponse;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import lombok.Getter;
import lombok.NonNull;
import org.apache.flink.util.ConfigurationException;

import com.getindata.connectors.http.internal.config.HttpConnectorConfigConstants;

@Getter
public class HttpResponseChecker {

    private final Set<Integer> successCodes;
    private final Set<Integer> temporalErrorCodes;
    private final Set<Integer> ignoreCodes;

    public HttpResponseChecker(
            @NonNull String successCodeExpr,
            @NonNull String temporalErrorCodeExpr,
            @NonNull String ignoreCodeExpr
    ) throws ConfigurationException {
        this(
            HttpCodesParser.parse(successCodeExpr),
            HttpCodesParser.parse(temporalErrorCodeExpr),
            HttpCodesParser.parse(ignoreCodeExpr)
        );
    }

    public HttpResponseChecker(
            @NonNull Set<Integer> successCodes,
            @NonNull Set<Integer> temporalErrorCodes,
            @NonNull Set<Integer> ignoreCodes
    ) throws ConfigurationException {
        this.successCodes = successCodes;
        this.temporalErrorCodes = temporalErrorCodes;
        this.ignoreCodes = ignoreCodes;
        validate();
    }

    public boolean isSuccessful(HttpResponse<?> response) {
        return isSuccessful(response.statusCode());
    }

    public boolean isSuccessful(int httpStatusCode) {
        return successCodes.contains(httpStatusCode) || ignoreCodes.contains(httpStatusCode);
    }

    public boolean isTemporalError(HttpResponse<?> response) {
        return isTemporalError(response.statusCode());
    }

    public boolean isTemporalError(int httpStatusCode) {
        return temporalErrorCodes.contains(httpStatusCode);
    }

    public boolean isIgnoreCode(HttpResponse<?> response) {
        return isIgnoreCode(response.statusCode());
    }

    public boolean isIgnoreCode(int httpStatusCode) {
        return ignoreCodes.contains(httpStatusCode);
    }

    public boolean isErrorCode(HttpResponse<?> response) {
        return isErrorCode(response.statusCode());
    }

    public boolean isErrorCode(int httpStatusCode) {
        return !isTemporalError(httpStatusCode) && !isSuccessful(httpStatusCode);
    }

    /**
     * Creates an {@link HttpResponseChecker} from sink {@link Properties}, handling backwards
     * compatibility with deprecated error code configuration keys.
     */
    public static HttpResponseChecker fromSinkProperties(Properties properties) {
        try {
            String deprecatedIgnoreExpr = properties.getProperty(
                    HttpConnectorConfigConstants.HTTP_ERROR_SINK_CODE_WHITE_LIST, "");
            String deprecatedErrorExpr = properties.getProperty(
                    HttpConnectorConfigConstants.HTTP_ERROR_SINK_CODES_LIST, "");

            boolean hasDeprecatedConfig = !deprecatedIgnoreExpr.replace(',', ' ').trim().isEmpty()
                    || !deprecatedErrorExpr.replace(',', ' ').trim().isEmpty();
            boolean hasNewConfig = properties.containsKey(HttpConnectorConfigConstants.SINK_SUCCESS_CODES)
                    || properties.containsKey(HttpConnectorConfigConstants.SINK_RETRY_CODES)
                    || properties.containsKey(HttpConnectorConfigConstants.SINK_IGNORE_RESPONSE_CODES);

            if (hasDeprecatedConfig && hasNewConfig) {
                throw new IllegalArgumentException(
                    "Cannot use deprecated sink error code options ("
                        + HttpConnectorConfigConstants.HTTP_ERROR_SINK_CODE_WHITE_LIST + ", "
                        + HttpConnectorConfigConstants.HTTP_ERROR_SINK_CODES_LIST
                        + ") together with new options ("
                        + HttpConnectorConfigConstants.SINK_SUCCESS_CODES + ", "
                        + HttpConnectorConfigConstants.SINK_RETRY_CODES + ", "
                        + HttpConnectorConfigConstants.SINK_IGNORE_RESPONSE_CODES + ").");
            }

            if (hasDeprecatedConfig) {
                return fromSinkPropertiesBackwardsCompatible(properties);
            } else {
                return fromSinkPropertiesWithDefaults(properties);
            }
        } catch (ConfigurationException e) {
            throw new IllegalStateException(e);
        }
    }

    private static HttpResponseChecker fromSinkPropertiesWithDefaults(Properties properties)
            throws ConfigurationException {
        String ignoreCodeExpr = properties.getProperty(
                HttpConnectorConfigConstants.SINK_IGNORE_RESPONSE_CODES, "");
        String retryCodeExpr = properties.getProperty(
                HttpConnectorConfigConstants.SINK_RETRY_CODES, "500,503,504");
        String successCodeExpr = properties.getProperty(
                HttpConnectorConfigConstants.SINK_SUCCESS_CODES, "1XX,2XX,3XX");
        return new HttpResponseChecker(successCodeExpr, retryCodeExpr, ignoreCodeExpr);
    }

    private static HttpResponseChecker fromSinkPropertiesBackwardsCompatible(Properties properties)
            throws ConfigurationException {
        String ignoreCodeExpr = properties.getProperty(
                HttpConnectorConfigConstants.HTTP_ERROR_SINK_CODE_WHITE_LIST, "");
        String errorCodeExpr = properties.getProperty(
                HttpConnectorConfigConstants.HTTP_ERROR_SINK_CODES_LIST, "4XX,5XX");

        // backwards compatibility
        var ignoreErrorCodes = HttpCodesParser.parse(ignoreCodeExpr);
        var errorCodes = HttpCodesParser.parse(errorCodeExpr);
        var retryCodes = HttpCodesParser.parse("500,503,504");
        var successCodes = new HashSet<>(HttpCodesParser.parse("1XX,2XX,3XX,4XX,5XX"));
        successCodes.removeAll(retryCodes);
        successCodes.removeAll(errorCodes);
        return new HttpResponseChecker(successCodes, retryCodes, ignoreErrorCodes);
    }

    private void validate() throws ConfigurationException {
        if (successCodes.isEmpty() && ignoreCodes.isEmpty()) {
            throw new ConfigurationException("Success and ignore code lists can not be empty");
        }
        HashSet<Integer> intersection = new HashSet<>(temporalErrorCodes);

        HashSet<Integer> combinedSuccessIgnoreCodes = new HashSet<>();
        combinedSuccessIgnoreCodes.addAll(successCodes);
        combinedSuccessIgnoreCodes.addAll(ignoreCodes);

        intersection.retainAll(combinedSuccessIgnoreCodes);
        if (!intersection.isEmpty()) {
            throw new ConfigurationException("Http codes " + intersection +
                    " can not be used as both success and retry codes");
        }
    }
}
