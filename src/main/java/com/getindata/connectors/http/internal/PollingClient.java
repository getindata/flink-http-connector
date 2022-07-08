package com.getindata.connectors.http.internal;

import com.getindata.connectors.http.internal.table.lookup.LookupArg;
import java.util.List;

public interface PollingClient<T> {

  T pull(List<LookupArg> params);
}
