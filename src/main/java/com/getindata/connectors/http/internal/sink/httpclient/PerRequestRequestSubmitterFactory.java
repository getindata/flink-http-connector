package com.getindata.connectors.http.internal.sink.httpclient;

import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.flink.util.concurrent.ExecutorThreadFactory;

import com.getindata.connectors.http.internal.utils.JavaNetHttpClientFactory;
import com.getindata.connectors.http.internal.utils.ThreadUtils;

public class PerRequestRequestSubmitterFactory implements RequestSubmitterFactory {

    // TODO Add this property to config. Make sure to add note in README.md that will describe that
    //  any value greater than one will break order of messages.
    int HTTP_CLIENT_THREAD_POOL_SIZE = 1;

    @Override
    public RequestSubmitter createSubmitter(Properties properties, String[] headersAndValues) {

        ExecutorService httpClientExecutor =
            Executors.newFixedThreadPool(
                HTTP_CLIENT_THREAD_POOL_SIZE,
                new ExecutorThreadFactory(
                    "http-sink-client-per-request-worker", ThreadUtils.LOGGING_EXCEPTION_HANDLER));

        return new PerRequestSubmitter(
                properties,
                headersAndValues,
                JavaNetHttpClientFactory.createClient(properties, httpClientExecutor)
            );
    }
}
