package com.getindata.connectors.http.internal.sink.httpclient;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpClient.Version;
import java.net.http.HttpRequest;
import java.net.http.HttpRequest.BodyPublisher;
import java.net.http.HttpRequest.BodyPublishers;
import java.net.http.HttpRequest.Builder;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.annotation.VisibleForTesting;

import com.getindata.connectors.http.internal.config.HttpConnectorConfigConstants;
import com.getindata.connectors.http.internal.sink.HttpSinkRequestEntry;

@Slf4j
public class BatchRequestSubmitter extends AbstractRequestSubmitter {

    private final int httpReqeustBatchSize;

    public BatchRequestSubmitter(
            Properties properties,
            String[] headersAndValue,
            HttpClient httpClient) {

        super(properties, headersAndValue, httpClient);

        this.httpReqeustBatchSize = Integer.parseInt(
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

        var responseFutures = new ArrayList<CompletableFuture<JavaNetHttpResponseWrapper>>();

        int counter = 0;
        String previousReqeustMethod = requestsToSubmit.get(0).method;
        List<HttpSinkRequestEntry> reqeustBatch = new ArrayList<>(httpReqeustBatchSize);
        for (var entry : requestsToSubmit) {
            if (!previousReqeustMethod.equalsIgnoreCase(entry.method)) {
                // break batch and submit
                responseFutures.add(sendBatch(endpointUrl, reqeustBatch));
                reqeustBatch.clear();
                // start a new batch for new HTTP method.
                reqeustBatch.add(entry);
            } else {
                reqeustBatch.add(entry);
                if (++counter % httpReqeustBatchSize == 0) {
                    // batch is full, submit and start new batch.
                    responseFutures.add(sendBatch(endpointUrl, reqeustBatch));
                    reqeustBatch.clear();
                }
            }
            previousReqeustMethod = entry.method;
        }

        if (!reqeustBatch.isEmpty()) {
            // submit anything that left
            responseFutures.add(sendBatch(endpointUrl, reqeustBatch));
        }
        return responseFutures;
    }

    @VisibleForTesting
    int getBatchSize() {
        return httpReqeustBatchSize;
    }

    private CompletableFuture<JavaNetHttpResponseWrapper> sendBatch(
            String endpointUrl,
            List<HttpSinkRequestEntry> reqeustBatch) {

        var endpointUri = URI.create(endpointUrl);
        return httpClient
            .sendAsync(
                buildHttpRequest(reqeustBatch, endpointUri),
                HttpResponse.BodyHandlers.ofString())
            .exceptionally(ex -> {
                // TODO This will be executed on a ForkJoinPool Thread... refactor this someday.
                log.error("Request fatally failed because of an exception", ex);
                return null;
            })
            .thenApplyAsync(
                // TODO HTTP-42
                res -> new JavaNetHttpResponseWrapper(reqeustBatch.get(0), res),
                publishingThreadPool
            );
    }

    private HttpRequest buildHttpRequest(List<HttpSinkRequestEntry> reqeustBatch, URI endpointUri) {

        try {
            var method = reqeustBatch.get(0).method;
            List<byte[]> elements = new ArrayList<>(reqeustBatch.size());

            BodyPublisher publisher;
            // By default, Java's BodyPublishers.ofByteArrays(elements) will just put Jsons
            // into the HTTP body without any context.
            // What we do here is we pack every Json/byteArray into Json Array hence '[' and ']'
            // at the end, and we separate every element with comma.
            elements.add("[".getBytes(StandardCharsets.UTF_8));
            for (HttpSinkRequestEntry entry : reqeustBatch) {
                elements.add(entry.element);
                elements.add(",".getBytes(StandardCharsets.UTF_8));
            }
            elements.set(elements.size() - 1, "]".getBytes(StandardCharsets.UTF_8));
            publisher = BodyPublishers.ofByteArrays(elements);

            Builder requestBuilder = HttpRequest
                .newBuilder()
                .uri(endpointUri)
                .version(Version.HTTP_1_1)
                .timeout(Duration.ofSeconds(httpRequestTimeOutSeconds))
                .method(method, publisher);

            if (headersAndValues.length != 0) {
                requestBuilder.headers(headersAndValues);
            }

            return requestBuilder.build();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
