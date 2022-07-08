package com.getindata.connectors.http.internal;

import java.io.Serializable;

/**
 * Builder building {@link SinkHttpClient}.
 */
public interface SinkHttpClientBuilder extends Serializable {

    SinkHttpClient build();
}
