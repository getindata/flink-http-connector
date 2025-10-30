package com.getindata.connectors.http.internal.sink.httpclient;

import java.net.http.HttpClient;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.util.ConfigurationException;

import com.getindata.connectors.http.HttpPostRequestCallback;
import com.getindata.connectors.http.internal.HeaderPreprocessor;
import com.getindata.connectors.http.internal.HttpLogger;
import com.getindata.connectors.http.internal.SinkHttpClient;
import com.getindata.connectors.http.internal.SinkHttpClientResponse;
import com.getindata.connectors.http.internal.config.HttpConnectorConfigConstants;
import com.getindata.connectors.http.internal.config.ResponseItemStatus;
import com.getindata.connectors.http.internal.sink.HttpSinkRequestEntry;
import com.getindata.connectors.http.internal.status.HttpCodesParser;
import com.getindata.connectors.http.internal.status.HttpResponseChecker;
import com.getindata.connectors.http.internal.utils.HttpHeaderUtils;

/**
 * An implementation of {@link SinkHttpClient} that uses Java 11's {@link HttpClient}. This
 * implementation supports HTTP traffic only.
 */
@Slf4j
public class JavaNetSinkHttpClient implements SinkHttpClient {

    private final String[] headersAndValues;

    private final Map<String, String> headerMap;

    private final HttpResponseChecker responseChecker;

    private final HttpPostRequestCallback<HttpRequest> httpPostRequestCallback;

    private final RequestSubmitter requestSubmitter;

    private final Properties properties;

    public JavaNetSinkHttpClient(
            Properties properties,
            HttpPostRequestCallback<HttpRequest> httpPostRequestCallback,
            HeaderPreprocessor headerPreprocessor,
            RequestSubmitterFactory requestSubmitterFactory) {

        this.httpPostRequestCallback = httpPostRequestCallback;
        this.headerMap = HttpHeaderUtils.prepareHeaderMap(
            HttpConnectorConfigConstants.SINK_HEADER_PREFIX,
            properties,
            headerPreprocessor
        );

        this.responseChecker = createHttpResponseChecker(properties);

        this.headersAndValues = HttpHeaderUtils.toHeaderAndValueArray(this.headerMap);
        this.requestSubmitter = requestSubmitterFactory.createSubmitter(
            properties,
            headersAndValues
        );
        this.properties = properties;
    }

    @Override
    public void open() {
        this.httpPostRequestCallback.open();
    }

    @Override
    public void close() {
        this.httpPostRequestCallback.close();
    }

    public static HttpResponseChecker createHttpResponseChecker(Properties properties) {
        try {
            String deprecatedIgnoreExpr = properties.getProperty(
                    HttpConnectorConfigConstants.HTTP_ERROR_SINK_CODE_WHITE_LIST,
                    ""
            );
            String deprecatedErrorExpr = properties.getProperty(
                    HttpConnectorConfigConstants.HTTP_ERROR_SINK_CODES_LIST,
                    ""
            );

            if (deprecatedIgnoreExpr.replace(',',' ').trim().isEmpty()
                    && deprecatedErrorExpr.replace(',',' ').trim().isEmpty()) {
                return createHttpResponseCheckerWithDefaults(properties);
            } else {
                return createBackwardsCompatibleResponseChecker(properties);
            }
        } catch (ConfigurationException e) {
            throw new IllegalStateException(e);
        }
    }

    private static HttpResponseChecker createHttpResponseCheckerWithDefaults(Properties properties)
            throws ConfigurationException {
        String ignoreCodeExpr = properties.getProperty(
                HttpConnectorConfigConstants.SINK_IGNORE_RESPONSE_CODES,
                ""
        );
        String retryCodeExpr = properties.getProperty(
                HttpConnectorConfigConstants.SINK_RETRY_CODES,
                "500,503,504"
        );
        String successCodeExpr = properties.getProperty(
                HttpConnectorConfigConstants.SINK_SUCCESS_CODES,
                "1XX,2XX,3XX"
        );

        return new HttpResponseChecker(successCodeExpr, retryCodeExpr, ignoreCodeExpr);
    }

    private static HttpResponseChecker createBackwardsCompatibleResponseChecker(Properties properties)
            throws ConfigurationException {
        String ignoreCodeExpr = properties.getProperty(
                HttpConnectorConfigConstants.HTTP_ERROR_SINK_CODE_WHITE_LIST,
                ""
        );
        String errorCodeExpr = properties.getProperty(
                HttpConnectorConfigConstants.HTTP_ERROR_SINK_CODES_LIST,
                "4XX,5XX"
        );

        //backwards compatibility
        var ignoreErrorCodes = HttpCodesParser.parse(ignoreCodeExpr);
        var errorCodes = HttpCodesParser.parse(errorCodeExpr);
        var retryCodes = HttpCodesParser.parse("500,503,504");

        var successCodes = new HashSet<>(HttpCodesParser.parse("1XX,2XX,3XX,4XX,5XX"));
        successCodes.removeAll(retryCodes);
        successCodes.removeAll(errorCodes);
        return new HttpResponseChecker(successCodes, retryCodes, ignoreErrorCodes);
    }

    @Override
    public CompletableFuture<SinkHttpClientResponse> putRequests(
        List<HttpSinkRequestEntry> requestEntries,
        String endpointUrl) {
        return submitRequests(requestEntries, endpointUrl)
            .thenApply(responses -> prepareSinkHttpClientResponse(responses, endpointUrl));
    }

    private CompletableFuture<List<JavaNetHttpResponseWrapper>> submitRequests(
            List<HttpSinkRequestEntry> requestEntries,
            String endpointUrl) {

        var responseFutures = requestSubmitter.submit(endpointUrl, requestEntries);
        var allFutures = CompletableFuture.allOf(responseFutures.toArray(new CompletableFuture[0]));
        return allFutures.thenApply(_void -> responseFutures.stream().map(CompletableFuture::join)
            .collect(Collectors.toList()));
    }

    private SinkHttpClientResponse prepareSinkHttpClientResponse(
        List<JavaNetHttpResponseWrapper> responses,
        String endpointUrl) {
        var responseItems = new ArrayList<SinkHttpClientResponse.ResponseItem>();

        for (var response : responses) {
            var sinkRequestEntry = response.getHttpRequest();
            var optResponse = response.getResponse();
            HttpLogger.getHttpLogger(properties).logResponse(response.getResponse().get());
            httpPostRequestCallback.call(optResponse.orElse(null), sinkRequestEntry, endpointUrl, headerMap);

            final ResponseItemStatus status;
            if (optResponse.isEmpty() || responseChecker.isTemporalError(optResponse.get())) {
                status = ResponseItemStatus.TEMPORAL;
            } else if (responseChecker.isIgnoreCode(optResponse.get())) {
                status = ResponseItemStatus.IGNORE;
            } else if (responseChecker.isSuccessful(optResponse.get())) {
                status = ResponseItemStatus.SUCCESS;
            } else {
                status = ResponseItemStatus.FAILURE;
            }

            responseItems.add(new SinkHttpClientResponse.ResponseItem(sinkRequestEntry, status));
        }

        return new SinkHttpClientResponse(responseItems);
    }

    @VisibleForTesting
    String[] getHeadersAndValues() {
        return Arrays.copyOf(headersAndValues, headersAndValues.length);
    }
}
