package com.getindata.connectors.http.internal;

import com.getindata.connectors.http.internal.table.lookup.HttpLookupConfig;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.table.functions.FunctionContext;

public interface PollingClientFactory<OUT> {

  PollingClient<OUT> createPollClient(FunctionContext context,
      HttpLookupConfig options);

  PollingClient<OUT> createPollClient(SourceReaderContext readerContext);
}
