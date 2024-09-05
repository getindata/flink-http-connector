package com.getindata.connectors.http.internal;

import java.io.Serializable;

import com.getindata.connectors.http.internal.table.lookup.HttpLookupConfig;

public interface PollingClientFactory<OUT> extends Serializable {

    PollingClient<OUT> createPollClient(
        HttpLookupConfig options
    );
}
