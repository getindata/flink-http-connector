package com.getindata.connectors.http.internal.table.lookup;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import lombok.Builder;
import lombok.Data;
import lombok.RequiredArgsConstructor;

@Builder
@Data
@RequiredArgsConstructor
public class HttpLookupConfig implements Serializable {

    private final String url;

    @Builder.Default
    private final List<String> arguments = Collections.emptyList();

    @Builder.Default
    private final boolean useAsync = false;

    @Builder.Default
    private final Properties properties = new Properties();
}
