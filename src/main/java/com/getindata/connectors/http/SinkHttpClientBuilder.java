package com.getindata.connectors.http;

import java.io.Serializable;

/**
 * Builder building {@link SinkHttpClient}.
 */
public interface SinkHttpClientBuilder extends Serializable {
  SinkHttpClient build();
}
