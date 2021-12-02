package com.getindata.connectors.http.table;

import com.getindata.connectors.http.JsonResultTableConverter.HttpResultConverterOptions;
import com.getindata.connectors.http.PollingClient;
import com.getindata.connectors.http.PollingClientFactory;
import java.io.Serializable;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.FunctionContext;

public abstract class AbstractTablePollingClientFactory
    implements PollingClientFactory<RowData>, Serializable {

  @Override
  public PollingClient<RowData> createPollClient(FunctionContext context, HttpLookupConfig options) {

    HttpResultConverterOptions converterOptions =
        HttpResultConverterOptions.builder()
            .root(options.getRoot())
            .aliases(options.getAliasPaths())
            .columnNames(options.getColumnNames())
            .build();

    return createRowDataPollClient(options, converterOptions);
  }

  protected abstract PollingClient<RowData> createRowDataPollClient(
      HttpLookupConfig options, HttpResultConverterOptions converterOptions);
}
