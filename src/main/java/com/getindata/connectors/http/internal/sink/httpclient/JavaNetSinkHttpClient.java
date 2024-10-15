package com.getindata.connectors.http.internal.sink.httpclient;

import java.net.http.HttpClient;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.annotation.VisibleForTesting;

import com.getindata.connectors.http.HttpPostRequestCallback;
import com.getindata.connectors.http.internal.HeaderPreprocessor;
import com.getindata.connectors.http.internal.SinkHttpClient;
import com.getindata.connectors.http.internal.SinkHttpClientResponse;
import com.getindata.connectors.http.internal.config.HttpConnectorConfigConstants;
import com.getindata.connectors.http.internal.sink.HttpSinkRequestEntry;
import com.getindata.connectors.http.internal.status.ComposeHttpStatusCodeChecker;
import com.getindata.connectors.http.internal.status.ComposeHttpStatusCodeChecker.ComposeHttpStatusCodeCheckerConfig;
import com.getindata.connectors.http.internal.status.HttpResponseStatus;
import com.getindata.connectors.http.internal.status.HttpStatusCodeChecker;
import com.getindata.connectors.http.internal.utils.HttpHeaderUtils;
import static com.getindata.connectors.http.internal.config.HttpConnectorConfigConstants.HTTP_ERROR_SINK_CODES_LIST;
import static com.getindata.connectors.http.internal.config.HttpConnectorConfigConstants.HTTP_ERROR_SINK_CODE_WHITE_LIST;

/**
 * An implementation of {@link SinkHttpClient} that uses Java 11's {@link HttpClient}. This
 * implementation supports HTTP traffic only.
 */
@Slf4j
public class JavaNetSinkHttpClient implements SinkHttpClient {

    private final String[] headersAndValues;

    private final Map<String, String> headerMap;

    private final HttpStatusCodeChecker statusCodeChecker;

    private final HttpPostRequestCallback<HttpRequest> httpPostRequestCallback;

    private final RequestSubmitter requestSubmitter;

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

        // TODO Inject this via constructor when implementing a response processor.
        //  Processor will be injected and it will wrap statusChecker implementation.
        ComposeHttpStatusCodeCheckerConfig checkerConfig =
            ComposeHttpStatusCodeCheckerConfig.builder()
                .properties(properties)
                .deprecatedErrorWhiteListPrefix(HTTP_ERROR_SINK_CODE_WHITE_LIST)
                .deprecatedCodePrefix(HTTP_ERROR_SINK_CODES_LIST)
                .errorWhiteListPrefix("")      // TODO: sink not refactored yet
                .errorCodePrefix("")
                .retryableWhiteListPrefix("")
                .retryableCodePrefix("")
                .build();

        this.statusCodeChecker = new ComposeHttpStatusCodeChecker(checkerConfig);

        this.headersAndValues = HttpHeaderUtils.toHeaderAndValueArray(this.headerMap);
        this.requestSubmitter = requestSubmitterFactory.createSubmitter(
            properties,
            headersAndValues
        );
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
        var successfulResponses = new ArrayList<HttpRequest>();
        var failedResponses = new ArrayList<HttpRequest>();

        for (var response : responses) {
            var sinkRequestEntry = response.getHttpRequest();
            var optResponse = response.getResponse();

            httpPostRequestCallback.call(
                optResponse.orElse(null), sinkRequestEntry, endpointUrl, headerMap);

            // TODO Add response processor here and orchestrate it with statusCodeChecker.
            if (optResponse.isPresent() &&
                statusCodeChecker.checkStatus(optResponse.get().statusCode())
                    .equals(HttpResponseStatus.SUCCESS)) {
                successfulResponses.add(sinkRequestEntry);
            } else {
                failedResponses.add(sinkRequestEntry);
            }
        }

        return new SinkHttpClientResponse(successfulResponses, failedResponses);
    }

    @VisibleForTesting
    String[] getHeadersAndValues() {
        return Arrays.copyOf(headersAndValues, headersAndValues.length);
    }
}
