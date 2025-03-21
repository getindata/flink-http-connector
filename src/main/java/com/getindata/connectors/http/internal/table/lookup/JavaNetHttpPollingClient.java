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
import java.util.List;
import java.util.Map;
import java.util.Optional;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.NullNode;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.StringUtils;

import com.getindata.connectors.http.HttpPostRequestCallback;
import com.getindata.connectors.http.internal.HeaderPreprocessor;
import com.getindata.connectors.http.internal.PollingClient;
import com.getindata.connectors.http.internal.config.HttpConnectorConfigConstants;
import com.getindata.connectors.http.internal.status.ComposeHttpStatusCodeChecker;
import com.getindata.connectors.http.internal.status.ComposeHttpStatusCodeChecker.ComposeHttpStatusCodeCheckerConfig;
import com.getindata.connectors.http.internal.status.HttpStatusCodeChecker;
import com.getindata.connectors.http.internal.utils.HttpHeaderUtils;
import static com.getindata.connectors.http.internal.config.HttpConnectorConfigConstants.RESULT_TYPE;
import static com.getindata.connectors.http.internal.table.lookup.HttpLookupConnectorOptions.SOURCE_LOOKUP_OIDC_AUTH_TOKEN_REQUEST;

/**
 * An implementation of {@link PollingClient} that uses Java 11's {@link HttpClient}.
 * This implementation supports HTTP traffic only.
 */
@Slf4j
public class JavaNetHttpPollingClient implements PollingClient<RowData> {

    private static final String RESULT_TYPE_SINGLE_VALUE = "single-value";
    private static final String RESULT_TYPE_ARRAY = "array";

    private final HttpClient httpClient;

    private final HttpStatusCodeChecker statusCodeChecker;

    private final DeserializationSchema<RowData> responseBodyDecoder;

    private final HttpRequestFactory requestFactory;

    private final ObjectMapper objectMapper;

    private final HttpPostRequestCallback<HttpLookupSourceRequestEntry> httpPostRequestCallback;

    private final HttpLookupConfig options;

    public JavaNetHttpPollingClient(
            HttpClient httpClient,
            DeserializationSchema<RowData> responseBodyDecoder,
            HttpLookupConfig options,
            HttpRequestFactory requestFactory) {

        this.httpClient = httpClient;
        this.responseBodyDecoder = responseBodyDecoder;
        this.requestFactory = requestFactory;

        this.objectMapper = new ObjectMapper();
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
        this.options = options;
    }

    @Override
    public Collection<RowData> pull(RowData lookupRow) {
        try {
            log.debug("Collection<RowData> pull with Rowdata={}.", lookupRow);
            return queryAndProcess(lookupRow);
        } catch (Exception e) {
            log.error("Exception during HTTP request.", e);
            return Collections.emptyList();
        }
    }

    // TODO Add Retry Policy And configure TimeOut from properties
    private Collection<RowData> queryAndProcess(RowData lookupData) throws Exception {

        HttpLookupSourceRequestEntry request = requestFactory.buildLookupRequest(lookupData);
        HttpResponse<String> response = httpClient.send(
            updateHttpRequestIfRequired(request,
                    HttpHeaderUtils.createOIDCHeaderPreprocessor(options.getReadableConfig())),
            BodyHandlers.ofString());
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

        if (response == null) {
            return Collections.emptyList();
        }

        String responseBody = response.body();
        int statusCode = response.statusCode();

        log.debug(String.format("Received status code [%s] for RestTableSource request " +
                        "with Server response body [%s] ", statusCode, responseBody));

        if (notErrorCodeAndNotEmptyBody(responseBody, statusCode)) {
            return deserialize(responseBody);
        } else {
            log.warn(
                String.format("Returned Http status code was invalid or returned body was empty. "
                + "Status Code [%s]", statusCode)
            );

            return Collections.emptyList();
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
}
