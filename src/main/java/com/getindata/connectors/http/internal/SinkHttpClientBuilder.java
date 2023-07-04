package com.getindata.connectors.http.internal;

import java.io.Serializable;
import java.util.Properties;

import org.apache.flink.annotation.PublicEvolving;

import com.getindata.connectors.http.HttpPostRequestCallback;
import com.getindata.connectors.http.internal.sink.httpclient.HttpRequest;
import com.getindata.connectors.http.internal.sink.httpclient.RequestSubmitterFactory;

/**
 * Builder building {@link SinkHttpClient}.
 */
@PublicEvolving
public interface SinkHttpClientBuilder extends Serializable {

    // TODO Consider moving HttpPostRequestCallback and HeaderPreprocessor, RequestSubmitter to be a
    //  SinkHttpClientBuilder fields. This method is getting more and more arguments.
    SinkHttpClient build(
        Properties properties,
        HttpPostRequestCallback<HttpRequest> httpPostRequestCallback,
        HeaderPreprocessor headerPreprocessor,
        RequestSubmitterFactory requestSubmitterFactory

    );
}
