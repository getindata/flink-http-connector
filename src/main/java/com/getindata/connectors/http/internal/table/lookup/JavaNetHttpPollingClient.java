package com.getindata.connectors.http.internal.table.lookup;

import java.io.IOException;
import java.util.Collections;
import java.util.Optional;

import lombok.extern.slf4j.Slf4j;
import okhttp3.OkHttpClient;
import okhttp3.Response;
import okhttp3.ResponseBody;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.table.data.RowData;

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

    private final OkHttpClient httpClient;

    private final HttpStatusCodeChecker statusCodeChecker;

    private final DeserializationSchema<RowData> responseBodyDecoder;

    private final HttpRequestFactory requestFactory;

    private final HttpPostRequestCallback<HttpLookupSourceRequestEntry> httpPostRequestCallback;

    public JavaNetHttpPollingClient(
            OkHttpClient httpClient,
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
            log.debug("Optional<RowData> pull with Rowdata={}.", lookupRow);
            return queryAndProcess(lookupRow);
        } catch (Exception e) {
            log.error("Exception during HTTP request.", e);
            return Optional.empty();
        }
    }

    // TODO Add Retry Policy And configure TimeOut from properties
    private Optional<RowData> queryAndProcess(RowData lookupData) throws Exception {

        HttpLookupSourceRequestEntry request = requestFactory.buildLookupRequest(lookupData);
        Response response = httpClient.newCall(
                request.getHttpRequest()
        ).execute();
        return processHttpResponse(response, request);
    }

    private Optional<RowData> processHttpResponse(
            Response response,
            HttpLookupSourceRequestEntry request) throws IOException {

        this.httpPostRequestCallback.call(response, request, "endpoint", Collections.emptyMap());

        if (response == null) {
            return Optional.empty();
        }

        ResponseBody body = response.body();
        String responseBody = body == null ? StringUtils.EMPTY : body.string();
        int statusCode = response.code();

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
        return !(StringUtils.isBlank(body) || statusCodeChecker.isErrorCode(statusCode));
    }

    @VisibleForTesting
    HttpRequestFactory getRequestFactory() {
        return this.requestFactory;
    }
}
