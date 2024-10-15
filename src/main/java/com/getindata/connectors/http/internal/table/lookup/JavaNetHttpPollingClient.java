package com.getindata.connectors.http.internal.table.lookup;

import java.io.IOException;
import java.net.http.HttpClient;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.time.Duration;
import java.util.Collections;
import java.util.Optional;
import static com.getindata.connectors.http.internal.config.HttpConnectorConfigConstants.HTTP_ERROR_NON_RETRYABLE_SOURCE_LOOKUP_CODES_LIST;
import static com.getindata.connectors.http.internal.config.HttpConnectorConfigConstants.HTTP_ERROR_NON_RETRYABLE_SOURCE_LOOKUP_CODE_WHITE_LIST;
import static com.getindata.connectors.http.internal.config.HttpConnectorConfigConstants.HTTP_ERROR_SOURCE_LOOKUP_CODES_LIST;
import static com.getindata.connectors.http.internal.config.HttpConnectorConfigConstants.HTTP_ERROR_SOURCE_LOOKUP_CODE_WHITE_LIST;
import static java.lang.String.format;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.StringUtils;
import org.apache.flink.util.concurrent.ExponentialBackoffRetryStrategy;
import org.apache.flink.util.concurrent.FixedRetryStrategy;
import org.apache.flink.util.concurrent.RetryStrategy;

import com.getindata.connectors.http.HttpPostRequestCallback;
import com.getindata.connectors.http.internal.PollingClient;
import com.getindata.connectors.http.internal.config.RetryStrategyType;
import com.getindata.connectors.http.internal.status.ComposeHttpStatusCodeChecker;
import com.getindata.connectors.http.internal.status.ComposeHttpStatusCodeChecker.ComposeHttpStatusCodeCheckerConfig;
import com.getindata.connectors.http.internal.status.HttpResponseStatus;
import com.getindata.connectors.http.internal.status.HttpStatusCodeChecker;
import static com.getindata.connectors.http.internal.config.HttpConnectorConfigConstants.HTTP_ERROR_RETRYABLE_SOURCE_LOOKUP_CODES_LIST;
import static com.getindata.connectors.http.internal.config.HttpConnectorConfigConstants.HTTP_ERROR_RETRYABLE_SOURCE_LOOKUP_CODE_WHITE_LIST;
import static com.getindata.connectors.http.internal.table.lookup.HttpLookupConnectorOptions.LOOKUP_RESTART_STRATEGY_EXPONENTIAL_DELAY_ATTEMPTS;
import static com.getindata.connectors.http.internal.table.lookup.HttpLookupConnectorOptions.LOOKUP_RESTART_STRATEGY_EXPONENTIAL_DELAY_INITIAL_DELAY;
import static com.getindata.connectors.http.internal.table.lookup.HttpLookupConnectorOptions.LOOKUP_RESTART_STRATEGY_EXPONENTIAL_DELAY_MAX_DELAY;
import static com.getindata.connectors.http.internal.table.lookup.HttpLookupConnectorOptions.LOOKUP_RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS;
import static com.getindata.connectors.http.internal.table.lookup.HttpLookupConnectorOptions.LOOKUP_RESTART_STRATEGY_FIXED_DELAY_DELAY;
import static com.getindata.connectors.http.internal.table.lookup.HttpLookupConnectorOptions.LOOKUP_RETRY_STRATEGY_TYPE;

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

    private final RetryStrategy retryStrategy;


    public JavaNetHttpPollingClient(
        HttpClient httpClient,
        DeserializationSchema<RowData> responseBodyDecoder,
        HttpLookupConfig options,
        HttpRequestFactory requestFactory) {

        this.httpClient = httpClient;
        this.responseBodyDecoder = responseBodyDecoder;
        this.requestFactory = requestFactory;

        this.httpPostRequestCallback = options.getHttpPostRequestCallback();
        this.retryStrategy = buildRestartStrategy(options.getReadableConfig());

        // TODO Inject this via constructor when implementing a response processor.
        //  Processor will be injected and it will wrap statusChecker implementation.
        ComposeHttpStatusCodeCheckerConfig checkerConfig =
            ComposeHttpStatusCodeCheckerConfig.builder()
                .properties(options.getProperties())
                .deprecatedErrorWhiteListPrefix(HTTP_ERROR_SOURCE_LOOKUP_CODE_WHITE_LIST)
                .deprecatedCodePrefix(HTTP_ERROR_SOURCE_LOOKUP_CODES_LIST)
                .errorWhiteListPrefix(HTTP_ERROR_NON_RETRYABLE_SOURCE_LOOKUP_CODE_WHITE_LIST)
                .errorCodePrefix(HTTP_ERROR_NON_RETRYABLE_SOURCE_LOOKUP_CODES_LIST)
                .retryableWhiteListPrefix(HTTP_ERROR_RETRYABLE_SOURCE_LOOKUP_CODE_WHITE_LIST)
                .retryableCodePrefix(HTTP_ERROR_RETRYABLE_SOURCE_LOOKUP_CODES_LIST)
                .build();

        this.statusCodeChecker = new ComposeHttpStatusCodeChecker(checkerConfig);
    }

    private RetryStrategy buildRestartStrategy(ReadableConfig readableConfig) {
        // RetryStrategy interface is not serializable, so we need to create it here.
        RetryStrategyType retryStrategyType = readableConfig.get(LOOKUP_RETRY_STRATEGY_TYPE);

        if (retryStrategyType.equals(RetryStrategyType.NONE)) {
            return new RetryStrategy() {
                @Override
                public int getNumRemainingRetries() {
                    return 0;
                }

                @Override
                public Duration getRetryDelay() {
                    return null;
                }

                @Override
                public RetryStrategy getNextRetryStrategy() {
                    return null;
                }
            };
        }
        if (retryStrategyType.equals(RetryStrategyType.FIXED_DELAY)) {
            return new FixedRetryStrategy(
                readableConfig.get(LOOKUP_RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS),
                readableConfig.get(LOOKUP_RESTART_STRATEGY_FIXED_DELAY_DELAY)
            );
        } else if (retryStrategyType.equals(RetryStrategyType.EXPONENTIAL_DELAY)) {
            return new ExponentialBackoffRetryStrategy(
                readableConfig.get(LOOKUP_RESTART_STRATEGY_EXPONENTIAL_DELAY_ATTEMPTS),
                readableConfig.get(LOOKUP_RESTART_STRATEGY_EXPONENTIAL_DELAY_INITIAL_DELAY),
                readableConfig.get(LOOKUP_RESTART_STRATEGY_EXPONENTIAL_DELAY_MAX_DELAY)
            );
        } else {
            throw new IllegalArgumentException(
                String.format("Invalid restart strategy type. Actual: %s. ",
                    retryStrategyType)
            );
        }
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

    private Optional<RowData> queryAndProcess(RowData lookupData) throws Exception {
        HttpLookupSourceRequestEntry request = requestFactory.buildLookupRequest(lookupData);

        RetryStrategy currentRetryStrategy = retryStrategy;
        final int maxRetryCount = currentRetryStrategy.getNumRemainingRetries();
        int tryCount = 0;

        do {
            tryCount++;
            HttpResponse<String> httpResponse = httpClient.send(
                request.getHttpRequest(),
                BodyHandlers.ofString()
            );
            Response parsedResponse = processHttpResponse(httpResponse, request);
            if (parsedResponse.getStatus() == HttpResponseStatus.SUCCESS
                || parsedResponse.getStatus() == HttpResponseStatus.FAILURE_NOT_RETRYABLE) {
                return parsedResponse.getRowData();
            } else {
                if (tryCount == maxRetryCount) {
                    log.error("Maximum retry count reached. Aborting...");
                } else {
                    log.info(
                        "Attempt {}/{} failed. Retrying HTTP request.",
                        tryCount,
                        maxRetryCount);
                    Thread.sleep(currentRetryStrategy.getRetryDelay().toMillis());
                }
            }
            currentRetryStrategy = currentRetryStrategy.getNextRetryStrategy();
        } while (currentRetryStrategy.getNumRemainingRetries() > 0);
        return Optional.empty();
    }

    private Response processHttpResponse(
        HttpResponse<String> response,
        HttpLookupSourceRequestEntry request) throws IOException {

        this.httpPostRequestCallback.call(response, request, "endpoint", Collections.emptyMap());

        if (response == null) {
            log.error("Empty HTTP response.");
            // TODO: when response is null?
            return Response.retryable();
        }

        String responseBody = response.body();
        int statusCode = response.statusCode();

        log.debug(format("Received status code [%s] for RestTableSource request " +
            "with Server response body [%s].", statusCode, responseBody));

        HttpResponseStatus httpResponseStatus = statusCodeChecker.checkStatus(statusCode);
        if (httpResponseStatus == HttpResponseStatus.SUCCESS) {
            log.trace("Returned successful status code [%s].");
            return Response.success(responseBodyDecoder.deserialize(responseBody.getBytes()));
        } else if (httpResponseStatus == HttpResponseStatus.FAILURE_NOT_RETRYABLE) {
            log.warn(format("Returned not retryable error status code [%s].", statusCode));
            return Response.notRetryable();
        } else if (httpResponseStatus == HttpResponseStatus.FAILURE_RETRYABLE) {
            log.warn(format("Returned retryable error status code [%s].", statusCode));
            return Response.retryable();
        } else if (StringUtils.isNullOrWhitespaceOnly(responseBody)) {
            // TODO: When it is possible?
            log.error(format("Returned body was empty. Status Code [%s].", statusCode));
            return Response.retryable();
        } else {
            throw new IllegalStateException(
                format("Unexpected state. Status Code [%s].", statusCode));
        }
    }

    @AllArgsConstructor
    private static final class Response {

        static Response success(RowData rowData) {
            return new Response(HttpResponseStatus.SUCCESS, rowData);
        }

        static Response notRetryable() {
            return new Response(HttpResponseStatus.FAILURE_NOT_RETRYABLE, null);
        }

        static Response retryable() {
            return new Response(HttpResponseStatus.FAILURE_RETRYABLE, null);
        }

        @Getter
        private final HttpResponseStatus status;
        private final RowData rowData;

        public Optional<RowData> getRowData() {
            return Optional.ofNullable(rowData);
        }
    }

    @VisibleForTesting
    HttpRequestFactory getRequestFactory() {
        return this.requestFactory;
    }
}
