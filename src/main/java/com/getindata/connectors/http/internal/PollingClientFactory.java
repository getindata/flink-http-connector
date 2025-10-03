package com.getindata.connectors.http.internal;

import java.io.Serializable;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.ConfigurationException;

import com.getindata.connectors.http.internal.table.lookup.HttpLookupConfig;

public interface PollingClientFactory extends Serializable {

    PollingClient createPollClient(
        HttpLookupConfig options,
        DeserializationSchema<RowData> schemaDecoder
    ) throws ConfigurationException;
}
