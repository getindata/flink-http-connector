package com.getindata.connectors.http.internal;

import java.io.Serializable;
import java.util.Properties;

import org.apache.flink.annotation.PublicEvolving;

import com.getindata.connectors.http.HttpPostRequestCallback;
import com.getindata.connectors.http.internal.sink.HttpSinkRequestEntry;

/**
 * Builder building {@link SinkHttpClient}.
 */
@PublicEvolving
public interface SinkHttpClientBuilder extends Serializable {

    // TODO Consider moving HttpPostRequestCallback and HeaderPreprocessor to be a
    //  SinkHttpClientBuilder fields. This method is getting more and more arguments.
    SinkHttpClient build(
        Properties properties,
        HttpPostRequestCallback<HttpSinkRequestEntry> httpPostRequestCallback,
        HeaderPreprocessor headerPreprocessor
    );
}
