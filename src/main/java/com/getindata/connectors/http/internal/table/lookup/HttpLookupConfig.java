package com.getindata.connectors.http.internal.table.lookup;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import lombok.Builder;
import lombok.Data;
import lombok.RequiredArgsConstructor;

@Builder
@Data
@RequiredArgsConstructor
public class HttpLookupConfig implements Serializable {

    private final String url;

    private final String root;

    @Builder.Default
    private final Map<String, String> aliasPaths = Collections.emptyMap();

    @Builder.Default
    private final List<String> arguments = Collections.emptyList();

    @Builder.Default
    private final List<String> columnNames = Collections.emptyList();

    @Builder.Default
    private final boolean useAsync = false;
}
