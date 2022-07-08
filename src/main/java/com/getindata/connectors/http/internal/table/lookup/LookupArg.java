package com.getindata.connectors.http.internal.table.lookup;

import lombok.Data;
import lombok.RequiredArgsConstructor;

@Data
@RequiredArgsConstructor
public class LookupArg {
  private final String argName;
  private final String argValue;
}
