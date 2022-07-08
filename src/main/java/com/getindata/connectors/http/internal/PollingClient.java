package com.getindata.connectors.http.internal;

import java.util.List;

import com.getindata.connectors.http.internal.table.lookup.LookupArg;

public interface PollingClient<T> {

    T pull(List<LookupArg> params);
}
