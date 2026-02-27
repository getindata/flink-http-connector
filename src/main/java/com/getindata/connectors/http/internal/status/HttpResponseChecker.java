package com.getindata.connectors.http.internal.status;

import java.net.http.HttpResponse;
import java.util.HashSet;
import java.util.Set;

import lombok.Getter;
import lombok.NonNull;
import org.apache.flink.util.ConfigurationException;

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
