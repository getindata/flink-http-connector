package com.getindata.connectors.http.internal.table.lookup;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.time.Duration;
import java.util.List;
import java.util.Optional;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.table.data.RowData;

import com.getindata.connectors.http.internal.PollingClient;
import com.getindata.connectors.http.internal.utils.uri.URIBuilder;

/**
 * An implementation of {@link PollingClient} that uses Java 11's {@link HttpClient}.
 * This implementation supports HTTP traffic only.
 */
@Slf4j
@RequiredArgsConstructor
public class RestTablePollingClient implements PollingClient<RowData> {

    private final HttpClient httpClient;

    private final DeserializationSchema<RowData> runtimeDecoder;

    private final HttpLookupConfig options;

    @Override
    public Optional<RowData> pull(List<LookupArg> lookupArgs) {
        try {
            return queryAndProcess(lookupArgs);
        } catch (Exception e) {
            log.error("Exception during HTTP request.", e);
            return Optional.empty();
        }
    }

    // TODO Add Retry Policy And configure TimeOut from properties
    private Optional<RowData> queryAndProcess(List<LookupArg> params) throws Exception {
        URI uri = buildUri(params);
        HttpRequest request =
            HttpRequest.newBuilder().uri(uri).GET().timeout(Duration.ofMinutes(2)).build();
        HttpResponse<String> response = httpClient.send(request, BodyHandlers.ofString());

        return processHttpResponse(response);
    }

    private URI buildUri(List<LookupArg> params) throws URISyntaxException {

        URIBuilder uriBuilder = new URIBuilder(options.getUrl());
        for (LookupArg arg : params) {
            uriBuilder.addParameter(arg.getArgName(), arg.getArgValue());
        }

        return uriBuilder.build();
    }

    // TODO Think about handling 2xx responses other than 200
    private Optional<RowData> processHttpResponse(HttpResponse<String> response)
            throws IOException {
        String body = response.body();
        int statusCode = response.statusCode();

        log.debug("Received {} status code for RestTableSource Request", statusCode);
        if (statusCode == 200) {
            log.trace("Server response body" + body);
            return Optional.ofNullable(runtimeDecoder.deserialize(body.getBytes()));
        } else {
            log.warn("Http Error Body - {}", body);
            return Optional.empty();
        }
    }
}
