package com.getindata.connectors.http.table;

import lombok.*;
import lombok.extern.slf4j.Slf4j;

import java.io.Serializable;

/**
 * The configuration for {@link HttpDynamicSink}, containing the data specific for the
 * {@link com.getindata.connectors.http.sink.HttpSink}.
 */
@Builder
@Data
@RequiredArgsConstructor
@Slf4j
public class HttpDynamicSinkConfig implements Serializable {
  /**
   * The URL of the endpoint.
   */
  @NonNull
  private final String url;

  /**
   * The name of the format used for encoding records.
   */
  @NonNull
  @Getter(AccessLevel.NONE)
  private final String format;

  /**
   * HTTP method name to use when sending the request.
   */
  @NonNull
  private final String insertMethod;

  /**
   * @return value of the Content-Type header based on a value set on the
   * {@link HttpDynamicSinkConfig#format} field,
   * e.g. <i>application/json</i> for <i>json</i> format.
   */
  public String getContentType() {
    switch (format) {
      case "json":
        return "application/json";
      default:
        log.warn("Unexpected format {}. MIME type for the request will be set to \"application/{}\".", format, format);
        return "application/" + format;
    }
  }
}
