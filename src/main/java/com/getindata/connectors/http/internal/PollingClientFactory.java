package com.getindata.connectors.http.internal;

import java.io.Serializable;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.util.ConfigurationException;

import com.getindata.connectors.http.internal.table.lookup.HttpLookupConfig;

public interface PollingClientFactory<OUT> extends Serializable {

    PollingClient<OUT> createPollClient(
        HttpLookupConfig options,
        DeserializationSchema<OUT> schemaDecoder
    ) throws ConfigurationException;
}
