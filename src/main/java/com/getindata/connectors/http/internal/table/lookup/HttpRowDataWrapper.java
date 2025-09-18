package com.getindata.connectors.http.internal.table.lookup;


import java.util.Collection;
import java.util.List;
import java.util.Map;

import lombok.Data;
import org.apache.flink.table.data.RowData;

/**
 * This bean contains the RowData information (the response body as a flink RowData).
 * It also contains information from the http response, namely the http headers map
 * and the http status code for the metadata columns.
 */
@Data
public class HttpRowDataWrapper {
    private final Collection<RowData> data;
    private final Map<String, List<String>> httpHeadersMap;
    private final Integer httpStatusCode;
    private final ContinueOnErrorType continueOnErrorType;
}
