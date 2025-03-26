package com.getindata.connectors.http.internal;

import java.io.Serializable;

import org.apache.flink.api.common.serialization.DeserializationSchema;

import com.getindata.connectors.http.internal.table.lookup.HttpLookupConfig;
import org.apache.flink.util.ConfigurationException;

public interface PollingClientFactory<OUT> extends Serializable {

    PollingClient<OUT> createPollClient(
        HttpLookupConfig options,
        DeserializationSchema<OUT> schemaDecoder
    ) throws ConfigurationException;
}
