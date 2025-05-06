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

    HttpResponseChecker(@NonNull String successCodeExpr, @NonNull String temporalErrorCodeExpr)
            throws ConfigurationException {
        this(HttpCodesParser.parse(successCodeExpr), HttpCodesParser.parse(temporalErrorCodeExpr));
    }

    public HttpResponseChecker(@NonNull Set<Integer> successCodes, @NonNull Set<Integer> temporalErrorCodes)
            throws ConfigurationException {
        this.successCodes = successCodes;
        this.temporalErrorCodes = temporalErrorCodes;
        validate();
    }

    public boolean isSuccessful(HttpResponse<?> response) {
        return isSuccessful(response.statusCode());
    }

    public boolean isSuccessful(int httpStatusCode) {
        return successCodes.contains(httpStatusCode);
    }

    public boolean isTemporalError(HttpResponse<?> response) {
        return isTemporalError(response.statusCode());
    }

    public boolean isTemporalError(int httpStatusCode) {
        return temporalErrorCodes.contains(httpStatusCode);
    }

    private void validate() throws ConfigurationException {
        if (successCodes.isEmpty()) {
            throw new ConfigurationException("Success code list can not be empty");
        }
        var intersection = new HashSet<>(successCodes);
        intersection.retainAll(temporalErrorCodes);
        if (!intersection.isEmpty()) {
            throw new ConfigurationException("Http codes " + intersection +
                    " can not be used as both success and retry codes");
        }
    }
}
