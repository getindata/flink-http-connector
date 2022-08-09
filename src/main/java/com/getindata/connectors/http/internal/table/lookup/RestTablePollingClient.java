package com.getindata.connectors.http.internal.table.lookup;

import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.time.Duration;
import java.util.List;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.table.data.RowData;

import com.getindata.connectors.http.internal.JsonResultTableConverter;
import com.getindata.connectors.http.internal.PollingClient;
import com.getindata.connectors.http.internal.utils.uri.URIBuilder;

@Slf4j
@RequiredArgsConstructor
public class RestTablePollingClient implements PollingClient<RowData> {

    private final JsonResultTableConverter resultConverter;
    private final HttpLookupConfig options;
    private final HttpClient httpClient;

    @Override
    public RowData pull(List<LookupArg> params) {
        try {
            return queryAndProcess(params);
        } catch (Exception e) {
            log.error("Exception during HTTP request.", e);
            return TableSourceHelper.buildEmptyRow(options.getColumnNames().size());
        }
    }

    // TODO Add Retry Policy And configure TimeOut from properties
    private RowData queryAndProcess(List<LookupArg> params) throws Exception {
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
    private RowData processHttpResponse(HttpResponse<String> response) {
        String body = response.body();
        int statusCode = response.statusCode();

        log.debug("Received {} status code for RestTableSource Request", statusCode);
        if (statusCode == 200) {
            return resultConverter.convert(body);
        } else {
            log.warn("Http Error Body - {}", body);
            return TableSourceHelper.buildEmptyRow(options.getColumnNames().size());
        }
    }
}
