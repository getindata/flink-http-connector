package com.getindata.connectors.http.internal.table.lookup;

import java.io.Serializable;
import java.util.Properties;

import lombok.Builder;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;

@Builder
@Data
@RequiredArgsConstructor
public class HttpLookupConfig implements Serializable {

    private final String lookupMethod;

    private final String url;

    @Builder.Default
    private final boolean useAsync = false;

    @Builder.Default
    private final Properties properties = new Properties();

    @Builder.Default
    private final ReadableConfig readableConfig = new Configuration();
}
