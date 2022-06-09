package com.getindata.connectors.http.sink;

import lombok.EqualsAndHashCode;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

import java.io.Serializable;

@RequiredArgsConstructor
@EqualsAndHashCode
public final class HttpSinkRequestEntry implements Serializable {
  @NonNull
  public final String method;
  @NonNull
  public final String contentType;
  public final byte[] element;

  public long getSizeInBytes() {
    return element.length;
  }
}
