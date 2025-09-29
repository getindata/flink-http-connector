package com.getindata.connectors.http.internal.table.lookup;


import java.util.Collection;
import java.util.List;
import java.util.Map;

import lombok.Builder;
import lombok.Data;
import org.apache.flink.table.data.RowData;

/**
 * This bean contains the RowData information (the response body as a flink RowData).
 * It also contains information from the http response, namely the http headers map
 * and the http status code where available. The extra information is for the metadata columns.
 */
@Builder
@Data
public class HttpRowDataWrapper {
    private final Collection<RowData> data;
    private final String errorMessage;
    private final Map<String, List<String>> httpHeadersMap;
    private final Integer httpStatusCode;
    private final HttpCompletionState httpCompletionState;

    public boolean shouldIgnore() {
        return  (this.data != null
                && this.data.isEmpty()
                && this.errorMessage == null
                && this.httpHeadersMap == null
                && this.httpStatusCode == null
                && httpCompletionState == HttpCompletionState.SUCCESS);
    }
}
