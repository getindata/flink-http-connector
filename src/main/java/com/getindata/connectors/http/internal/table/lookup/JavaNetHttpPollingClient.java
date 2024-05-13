package com.getindata.connectors.http.internal.table.lookup;

import java.io.IOException;
import java.net.http.HttpClient;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.util.Collections;
import java.util.Optional;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.StringUtils;

import com.getindata.connectors.http.HttpPostRequestCallback;
import com.getindata.connectors.http.internal.PollingClient;
import com.getindata.connectors.http.internal.config.HttpConnectorConfigConstants;
import com.getindata.connectors.http.internal.status.ComposeHttpStatusCodeChecker;
import com.getindata.connectors.http.internal.status.ComposeHttpStatusCodeChecker.ComposeHttpStatusCodeCheckerConfig;
import com.getindata.connectors.http.internal.status.HttpStatusCodeChecker;

/**
 * An implementation of {@link PollingClient} that uses Java 11's {@link HttpClient}.
 * This implementation supports HTTP traffic only.
 */
@Slf4j
public class JavaNetHttpPollingClient implements PollingClient<RowData> {

    private final HttpClient httpClient;

    private final HttpStatusCodeChecker statusCodeChecker;

    private final DeserializationSchema<RowData> responseBodyDecoder;

    private final HttpRequestFactory requestFactory;

    private final HttpPostRequestCallback<HttpLookupSourceRequestEntry> httpPostRequestCallback;

    public JavaNetHttpPollingClient(
            HttpClient httpClient,
            DeserializationSchema<RowData> responseBodyDecoder,
            HttpLookupConfig options,
            HttpRequestFactory requestFactory) {

        this.httpClient = httpClient;
        this.responseBodyDecoder = responseBodyDecoder;
        this.requestFactory = requestFactory;

        this.httpPostRequestCallback = options.getHttpPostRequestCallback();

        // TODO Inject this via constructor when implementing a response processor.
        //  Processor will be injected and it will wrap statusChecker implementation.
        ComposeHttpStatusCodeCheckerConfig checkerConfig =
            ComposeHttpStatusCodeCheckerConfig.builder()
                .properties(options.getProperties())
                .whiteListPrefix(
                    HttpConnectorConfigConstants.HTTP_ERROR_SOURCE_LOOKUP_CODE_WHITE_LIST
                )
                .errorCodePrefix(HttpConnectorConfigConstants.HTTP_ERROR_SOURCE_LOOKUP_CODES_LIST)
                .build();

        this.statusCodeChecker = new ComposeHttpStatusCodeChecker(checkerConfig);
    }

    @Override
    public Optional<RowData> pull(RowData lookupRow) {
        try {
            return queryAndProcess(lookupRow);
        } catch (Exception e) {
            log.error("Exception during HTTP request.", e);
            return Optional.empty();
        }
    }

    // TODO Add Retry Policy And configure TimeOut from properties
    private Optional<RowData> queryAndProcess(RowData lookupData) throws Exception {

        HttpLookupSourceRequestEntry request = requestFactory.buildLookupRequest(lookupData);
        HttpResponse<String> response = httpClient.send(
            request.getHttpRequest(),
            BodyHandlers.ofString()
        );
        return processHttpResponse(response, request);
    }

    private Optional<RowData> processHttpResponse(
            HttpResponse<String> response,
            HttpLookupSourceRequestEntry request) throws IOException {

        this.httpPostRequestCallback.call(response, request, "endpoint", Collections.emptyMap());

        if (response == null) {
            return Optional.empty();
        }

        String responseBody = response.body();
        int statusCode = response.statusCode();

        log.debug(String.format("Received status code [%s] for RestTableSource request " +
                        "with Server response body [%s] ", statusCode, responseBody));

        if (notErrorCodeAndNotEmptyBody(responseBody, statusCode)) {
            return Optional.ofNullable(responseBodyDecoder.deserialize(responseBody.getBytes()));
        } else {
            log.warn(
                String.format("Returned Http status code was invalid or returned body was empty. "
                + "Status Code [%s]", statusCode)
            );

            return Optional.empty();
        }
    }

    private boolean notErrorCodeAndNotEmptyBody(String body, int statusCode) {
        return !(StringUtils.isNullOrWhitespaceOnly(body) || statusCodeChecker.isErrorCode(
            statusCode));
    }

    @VisibleForTesting
    HttpRequestFactory getRequestFactory() {
        return this.requestFactory;
    }
}
