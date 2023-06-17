package com.getindata.connectors.http.internal.sink.httpclient;

import java.net.URI;
import java.net.http.HttpClient.Version;
import java.net.http.HttpRequest;
import java.net.http.HttpRequest.BodyPublisher;
import java.net.http.HttpRequest.BodyPublishers;
import java.net.http.HttpRequest.Builder;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;

import lombok.extern.slf4j.Slf4j;

import com.getindata.connectors.http.internal.config.HttpConnectorConfigConstants;
import com.getindata.connectors.http.internal.sink.HttpSinkRequestEntry;

@Slf4j
public class BatchRequestSubmitter extends AbstractRequestSubmitter {

    // TODO HTTP-42 we MUST use maxBatchSize from HttpSink here
    protected static final String DEFAULT_HTTP_BATCH_REQUEST_SIZE = "20";

    private final int httpReqeustBatchSize;

    public BatchRequestSubmitter(Properties properties, String[] headersAndValue) {
        super(properties, headersAndValue);

        this.httpReqeustBatchSize = Integer.parseInt(
            properties.getProperty(HttpConnectorConfigConstants.SINK_HTTP_BATCH_REQUEST_SIZE,
                DEFAULT_HTTP_BATCH_REQUEST_SIZE)
        );
    }

    @Override
    public List<CompletableFuture<JavaNetHttpResponseWrapper>> submit(
        String endpointUrl,
        List<HttpSinkRequestEntry> requestToSubmit) {

        var responseFutures = new ArrayList<CompletableFuture<JavaNetHttpResponseWrapper>>();

        int counter = 0;
        String previousReqeustMethod = requestToSubmit.get(0).method;
        List<HttpSinkRequestEntry> reqeustBatch = new ArrayList<>(httpReqeustBatchSize);
        for (var entry : requestToSubmit) {
            reqeustBatch.add(entry);
            if (++counter % httpReqeustBatchSize == 0
                || !previousReqeustMethod.equalsIgnoreCase(entry.method)) {
                responseFutures.add(sendBatch(endpointUrl, reqeustBatch));
                reqeustBatch.clear();
            }
        }

        if (!reqeustBatch.isEmpty()) {
            responseFutures.add(sendBatch(endpointUrl, reqeustBatch));
        }

        return responseFutures;
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
                // TODO This will be executed on a ForJoinPool Thread... refactor this someday.
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
            if (reqeustBatch.size() > 1) {
                // Buy default, Java's BodyPublishers.ofByteArrays(elements) will just put Jsons
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
            } else {
                publisher = BodyPublishers.ofByteArray(reqeustBatch.get(0).element);
            }

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
