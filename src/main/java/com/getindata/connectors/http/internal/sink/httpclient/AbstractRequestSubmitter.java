package com.getindata.connectors.http.internal.sink.httpclient;

import java.net.http.HttpClient;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.flink.util.concurrent.ExecutorThreadFactory;

import com.getindata.connectors.http.internal.config.HttpConnectorConfigConstants;
import com.getindata.connectors.http.internal.utils.JavaNetHttpClientFactory;
import com.getindata.connectors.http.internal.utils.ThreadUtils;

public abstract class AbstractRequestSubmitter implements RequestSubmitter {

    // TODO Add this property to config. Make sure to add note in README.md that will describe that
    //  any value greater than one will break order of messages.
    protected static final int HTTP_CLIENT_THREAD_POOL_SIZE = 1;

    protected static final int HTTP_CLIENT_PUBLISHING_THREAD_POOL_SIZE = 1;

    protected static final String DEFAULT_REQUEST_TIMEOUT_SECONDS = "30";

    /**
     * Thread pool to handle HTTP response from HTTP client.
     */
    protected final ExecutorService publishingThreadPool;

    protected final int httpRequestTimeOutSeconds;

    protected final String[] headersAndValues;

    protected final HttpClient httpClient;

    public AbstractRequestSubmitter(Properties properties, String[] headersAndValues) {

        this.headersAndValues = headersAndValues;
        this.publishingThreadPool =
            Executors.newFixedThreadPool(
                HTTP_CLIENT_PUBLISHING_THREAD_POOL_SIZE,
                new ExecutorThreadFactory(
                    "http-sink-client-response-worker", ThreadUtils.LOGGING_EXCEPTION_HANDLER));

        this.httpRequestTimeOutSeconds = Integer.parseInt(
            properties.getProperty(HttpConnectorConfigConstants.SINK_HTTP_TIMEOUT_SECONDS,
                DEFAULT_REQUEST_TIMEOUT_SECONDS)
        );

        ExecutorService httpClientExecutor =
            Executors.newFixedThreadPool(
                HTTP_CLIENT_THREAD_POOL_SIZE,
                new ExecutorThreadFactory(
                    "http-sink-client-request-worker", ThreadUtils.LOGGING_EXCEPTION_HANDLER));

        this.httpClient = JavaNetHttpClientFactory.createClient(properties, httpClientExecutor);
    }
}
