package com.getindata.connectors.http.internal.table.lookup;

import java.io.IOException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.NullNode;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.util.ConfigurationException;
import org.apache.flink.util.StringUtils;

import com.getindata.connectors.http.HttpPostRequestCallback;
import com.getindata.connectors.http.internal.HeaderPreprocessor;
import com.getindata.connectors.http.internal.PollingClient;
import com.getindata.connectors.http.internal.retry.HttpClientWithRetry;
import com.getindata.connectors.http.internal.retry.RetryConfigProvider;
import com.getindata.connectors.http.internal.status.HttpCodesParser;
import com.getindata.connectors.http.internal.status.HttpResponseChecker;
import com.getindata.connectors.http.internal.utils.HttpHeaderUtils;
import static com.getindata.connectors.http.internal.config.HttpConnectorConfigConstants.RESULT_TYPE;
import static com.getindata.connectors.http.internal.table.lookup.HttpLookupConnectorOptions.SOURCE_LOOKUP_HTTP_IGNORED_RESPONSE_CODES;
import static com.getindata.connectors.http.internal.table.lookup.HttpLookupConnectorOptions.SOURCE_LOOKUP_HTTP_RETRY_CODES;
import static com.getindata.connectors.http.internal.table.lookup.HttpLookupConnectorOptions.SOURCE_LOOKUP_HTTP_SUCCESS_CODES;
import static com.getindata.connectors.http.internal.table.lookup.HttpLookupConnectorOptions.SOURCE_LOOKUP_OIDC_AUTH_TOKEN_REQUEST;

/**
 * An implementation of {@link PollingClient} that uses Java 11's {@link HttpClient}.
 * This implementation supports HTTP traffic only.
 */
@Slf4j
public class JavaNetHttpPollingClient implements PollingClient<RowData> {

    private static final String RESULT_TYPE_SINGLE_VALUE = "single-value";
    private static final String RESULT_TYPE_ARRAY = "array";

    private final HttpClientWithRetry httpClient;
    private final DeserializationSchema<RowData> responseBodyDecoder;
    private final HttpRequestFactory requestFactory;
    private final ObjectMapper objectMapper;
    private final HttpPostRequestCallback<HttpLookupSourceRequestEntry> httpPostRequestCallback;
    private final HttpLookupConfig options;
    private final Set<Integer> ignoredErrorCodes;

    public JavaNetHttpPollingClient(
            HttpClient httpClient,
            DeserializationSchema<RowData> responseBodyDecoder,
            HttpLookupConfig options,
            HttpRequestFactory requestFactory) throws ConfigurationException {

        this.responseBodyDecoder = responseBodyDecoder;
        this.requestFactory = requestFactory;
        this.objectMapper = new ObjectMapper();
        this.httpPostRequestCallback = options.getHttpPostRequestCallback();
        this.options = options;

        var config = options.getReadableConfig();

        this.ignoredErrorCodes = HttpCodesParser.parse(config.get(SOURCE_LOOKUP_HTTP_IGNORED_RESPONSE_CODES));
        var errorCodes = HttpCodesParser.parse(config.get(SOURCE_LOOKUP_HTTP_RETRY_CODES));
        var successCodes = new HashSet<Integer>();
        successCodes.addAll(HttpCodesParser.parse(config.get(SOURCE_LOOKUP_HTTP_SUCCESS_CODES)));
        successCodes.addAll(ignoredErrorCodes);

        this.httpClient = HttpClientWithRetry.builder()
                .httpClient(httpClient)
                .retryConfig(RetryConfigProvider.create(config))
                .responseChecker(new HttpResponseChecker(successCodes, errorCodes))
                .build();
    }

    public void open(FunctionContext context) {
        httpClient.registerMetrics(context.getMetricGroup());
    }


    @Override
    public Collection<RowData> pull(RowData lookupRow) {
        if (lookupRow == null) {
            return Collections.emptyList();
        }
        try {
            log.debug("Collection<RowData> pull with Rowdata={}.", lookupRow);
            return queryAndProcess(lookupRow);
        } catch (Exception e) {
            throw new RuntimeException("Exception during HTTP request", e);
        }
    }

    private Collection<RowData> queryAndProcess(RowData lookupData) throws Exception {
        var request = requestFactory.buildLookupRequest(lookupData);

        var oidcProcessor = HttpHeaderUtils.createOIDCHeaderPreprocessor(options.getReadableConfig());
        var response = httpClient.send(
            () -> updateHttpRequestIfRequired(request, oidcProcessor), BodyHandlers.ofString());
        return processHttpResponse(response, request);
    }

    /**
     * If using OIDC, update the http request using the oidc header pre processor to supply the
     * authentication header, with a short lived bearer token.
     * @param request http reauest to amend
     * @param oidcHeaderPreProcessor OIDC header pre processor
     * @return http request, which for OIDC will have the bearer token as the authentication header
     */
    protected HttpRequest updateHttpRequestIfRequired(HttpLookupSourceRequestEntry request,
                                                      HeaderPreprocessor oidcHeaderPreProcessor) {
        // We need to check the config and if required amend the value of the
        // authentication header to the short lived bearer token
        HttpRequest httpRequest = request.getHttpRequest();
        ReadableConfig readableConfig = options.getReadableConfig();
        if (oidcHeaderPreProcessor != null) {
            HttpRequest.Builder builder = HttpRequest.newBuilder()
                    .uri(httpRequest.uri());
            if (httpRequest.timeout().isPresent()) {
                builder.timeout(httpRequest.timeout().get());
            }
            if (httpRequest.method().endsWith("GET")) {
                builder.GET();
            } else {
                builder.method(httpRequest.method(), httpRequest.bodyPublisher().get());
            }
            Map<String, String> headerMap = new HashMap<>();
            if (httpRequest.headers() != null && !httpRequest.headers().map().isEmpty()) {
                for (Map.Entry<String, List<String>> header
                        :httpRequest.headers().map().entrySet()) {
                    List<String> values =  header.getValue();
                    if (values.size() == 1) {
                        headerMap.put(header.getKey(), header.getValue().get(0));
                    }
                    // the existing design does not handle multiple values for headers
                }
            }
            Optional<String> oidcTokenRequest = readableConfig
                    .getOptional(SOURCE_LOOKUP_OIDC_AUTH_TOKEN_REQUEST);
            String bearerToken = oidcHeaderPreProcessor.preprocessValueForHeader(
                    HttpHeaderUtils.AUTHORIZATION,  oidcTokenRequest.get());
            headerMap.put(HttpHeaderUtils.AUTHORIZATION, bearerToken);
            String[] headerAndValueArray = HttpHeaderUtils.toHeaderAndValueArray(headerMap);
            builder.headers(headerAndValueArray);
            httpRequest = builder.build();
        }
        return httpRequest;
    }

    private Collection<RowData> processHttpResponse(
            HttpResponse<String> response,
            HttpLookupSourceRequestEntry request) throws IOException {

        this.httpPostRequestCallback.call(response, request, "endpoint", Collections.emptyMap());

        var responseBody = response.body();

        log.debug("Received status code [{}] for RestTableSource request with Server response body [{}] ",
                response.statusCode(), responseBody);

        if (StringUtils.isNullOrWhitespaceOnly(responseBody) || ignoreResponse(response)) {
            return Collections.emptyList();
        }
        return deserialize(responseBody);
    }

    @VisibleForTesting
    HttpRequestFactory getRequestFactory() {
        return this.requestFactory;
    }

    private Collection<RowData> deserialize(String responseBody) throws IOException {
        byte[] rawBytes = responseBody.getBytes();
        String resultType =
            options.getProperties().getProperty(RESULT_TYPE, RESULT_TYPE_SINGLE_VALUE);
        if (resultType.equals(RESULT_TYPE_SINGLE_VALUE)) {
            return deserializeSingleValue(rawBytes);
        } else if (resultType.equals(RESULT_TYPE_ARRAY)) {
            return deserializeArray(rawBytes);
        } else {
            throw new IllegalStateException(
                String.format("Unknown lookup source result type '%s'.", resultType));
        }
    }

    private List<RowData> deserializeSingleValue(byte[] rawBytes) throws IOException {
        return Optional.ofNullable(responseBodyDecoder.deserialize(rawBytes))
            .map(Collections::singletonList)
            .orElse(Collections.emptyList());
    }

    private List<RowData> deserializeArray(byte[] rawBytes) throws IOException {
        List<JsonNode> rawObjects =
            objectMapper.readValue(rawBytes, new TypeReference<>() {
            });
        List<RowData> result = new ArrayList<>();
        for (JsonNode rawObject : rawObjects) {
            if (!(rawObject instanceof NullNode)) {
                RowData deserialized =
                    responseBodyDecoder.deserialize(rawObject.toString().getBytes());
                // deserialize() returns null if deserialization fails
                if (deserialized != null) {
                    result.add(deserialized);
                }
            }
        }
        return result;
    }

    private boolean ignoreResponse(HttpResponse<?> response) {
        return ignoredErrorCodes.contains(response.statusCode());
    }
}
