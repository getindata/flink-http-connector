package com.getindata.connectors.http.internal.sink.httpclient;

import java.io.ByteArrayOutputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;

import com.getindata.connectors.http.internal.utils.HttpClientUtils;
import lombok.extern.slf4j.Slf4j;
import okhttp3.Headers;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import org.apache.flink.annotation.VisibleForTesting;

import com.getindata.connectors.http.internal.config.HttpConnectorConfigConstants;
import com.getindata.connectors.http.internal.sink.HttpSinkRequestEntry;

/**
 * This implementation groups received events in batches and submits each batch as individual HTTP
 * requests. Batch is created based on batch size or based on HTTP method type.
 */
@Slf4j
public class BatchRequestSubmitter extends AbstractRequestSubmitter {

    private static final byte[] BATCH_START_BYTES = "[".getBytes(StandardCharsets.UTF_8);

    private static final byte[] BATCH_END_BYTES = "]".getBytes(StandardCharsets.UTF_8);

    private static final byte[] BATCH_ELEMENT_DELIM_BYTES = ",".getBytes(StandardCharsets.UTF_8);

    private final int httpRequestBatchSize;

    public BatchRequestSubmitter(
            Properties properties,
            String[] headersAndValue,
            OkHttpClient httpClient) {

        super(properties, headersAndValue, httpClient);

        this.httpRequestBatchSize = Integer.parseInt(
            properties.getProperty(HttpConnectorConfigConstants.SINK_HTTP_BATCH_REQUEST_SIZE)
        );
    }

    @Override
    public List<CompletableFuture<JavaNetHttpResponseWrapper>> submit(
            String endpointUrl,
            List<HttpSinkRequestEntry> requestsToSubmit) {

        if (requestsToSubmit.isEmpty()) {
            return Collections.emptyList();
        }

        List<CompletableFuture<JavaNetHttpResponseWrapper>> responseFutures = new ArrayList<>();
        String previousReqeustMethod = requestsToSubmit.get(0).method;
        List<HttpSinkRequestEntry> requestBatch = new ArrayList<>(httpRequestBatchSize);

        for (HttpSinkRequestEntry entry : requestsToSubmit) {
            if (requestBatch.size() == httpRequestBatchSize
                || !previousReqeustMethod.equalsIgnoreCase(entry.method)) {
                // break batch and submit
                responseFutures.add(sendBatch(endpointUrl, requestBatch));
                requestBatch.clear();
            }
            requestBatch.add(entry);
            previousReqeustMethod = entry.method;
        }

        // submit anything that left
        responseFutures.add(sendBatch(endpointUrl, requestBatch));
        return responseFutures;
    }

    @VisibleForTesting
    int getBatchSize() {
        return httpRequestBatchSize;
    }

    private CompletableFuture<JavaNetHttpResponseWrapper> sendBatch(
            String endpointUrl,
            List<HttpSinkRequestEntry> reqeustBatch) {

        HttpRequest httpRequest = buildHttpRequest(reqeustBatch, URI.create(endpointUrl));
        return HttpClientUtils.sendAsyncRequest(httpClient, httpRequest.getHttpRequest())
            .exceptionally(ex -> {
                // TODO This will be executed on a ForkJoinPool Thread... refactor this someday.
                log.error("Request fatally failed because of an exception", ex);
                return null;
            })
            .thenApplyAsync(
                res -> new JavaNetHttpResponseWrapper(httpRequest, res),
                publishingThreadPool
            );
    }

    private HttpRequest buildHttpRequest(List<HttpSinkRequestEntry> requestBatch, URI endpointUri) {

        try {
            String method = requestBatch.get(0).method;
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

            // By default, Java's BodyPublishers.ofByteArrays(elements) will just put Jsons
            // into the HTTP body without any context.
            // What we do here is we pack every Json/byteArray into Json Array hence '[' and ']'
            // at the end, and we separate every element with comma.
            outputStream.write(BATCH_START_BYTES);
            for (int i = 0, last = requestBatch.size() - 1; i < requestBatch.size(); i++) {
                HttpSinkRequestEntry entry = requestBatch.get(i);
                outputStream.write(entry.element);
                if (i != last) {
                    outputStream.write(BATCH_ELEMENT_DELIM_BYTES);
                }
            }
            outputStream.write(BATCH_END_BYTES);

            Request.Builder requestBuilder = new Request.Builder()
                    .url(endpointUri.toURL().toString())
//                .(Version.HTTP_1_1)
//                    .writeTimeout(Duration.ofSeconds(httpRequestTimeOutSeconds))
                    .method(method, RequestBody.create(outputStream.toByteArray()));

            if (headersAndValues.length != 0) {
                requestBuilder.headers(Headers.of(headersAndValues));
            }

            return new HttpRequest(requestBuilder.build(), null, method);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
