package com.getindata.connectors.http.internal.table.lookup;

import java.io.IOException;
import java.net.http.HttpClient;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
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
import static com.getindata.connectors.http.internal.config.HttpConnectorConfigConstants.RESULT_TYPE;

/**
 * An implementation of {@link PollingClient} that uses Java 11's {@link HttpClient}.
 * This implementation supports HTTP traffic only.
 */
@Slf4j
public class JavaNetHttpPollingClient implements PollingClient<RowData> {

    private final HttpClient httpClient;

    private final HttpStatusCodeChecker statusCodeChecker;

    private final DeserializationSchema<RowData> responseBodyDecoder;

    private final ObjectMapper objectMapper;

    private final HttpRequestFactory requestFactory;

    private final HttpPostRequestCallback<HttpLookupSourceRequestEntry> httpPostRequestCallback;

    private final HttpLookupConfig options;

    public JavaNetHttpPollingClient(
            HttpClient httpClient,
            DeserializationSchema<RowData> responseBodyDecoder,
            ObjectMapper objectMapper,
            HttpLookupConfig options,
            HttpRequestFactory requestFactory) {

        this.httpClient = httpClient;
        this.responseBodyDecoder = responseBodyDecoder;
        this.objectMapper = objectMapper;
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
            request.getHttpRequest(),
            BodyHandlers.ofString()
        );
        return processHttpResponse(response, request);
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
        String resultType = options.getProperties().getProperty(RESULT_TYPE, "single-value");
        if (resultType.equals("single-value")) {
            return Collections.singletonList(responseBodyDecoder.deserialize(rawBytes));
        } else if (resultType.equals("array")) {
            List<JsonNode> rawObjects =
                objectMapper.readValue(rawBytes, new TypeReference<>() {
                });
            List<RowData> result = new ArrayList<>(rawObjects.size());
            for (JsonNode rawObject : rawObjects) {
                result.add(responseBodyDecoder.deserialize(rawObject.toString().getBytes()));
            }
            return result;
        } else {
            throw new IllegalStateException(
                String.format("Unknown lookup source result type '%'.", resultType));
        }
    }
}
