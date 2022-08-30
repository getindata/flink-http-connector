package com.getindata.connectors.http.internal;

import java.io.Serializable;
import java.util.Properties;

import org.apache.flink.annotation.PublicEvolving;

import com.getindata.connectors.http.HttpSinkPostRequestCallback;

/**
 * Builder building {@link SinkHttpClient}.
 */
@PublicEvolving
public interface SinkHttpClientBuilder extends Serializable {
    SinkHttpClient build(
        Properties properties,
        HttpSinkPostRequestCallback httpSinkPostRequestCallback
    );
}
