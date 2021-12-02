package com.getindata.connectors.http;

import com.getindata.connectors.http.table.LookupArg;
import java.util.List;

public interface PollingClient<T> {

  T pull(List<LookupArg> params);
}
