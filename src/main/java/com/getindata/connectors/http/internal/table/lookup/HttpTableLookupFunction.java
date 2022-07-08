package com.getindata.connectors.http.internal.table.lookup;

import com.getindata.connectors.http.internal.PollingClient;
import com.getindata.connectors.http.internal.PollingClientFactory;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.Data;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryStringData;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.TableFunction;

@Slf4j
public class HttpTableLookupFunction extends TableFunction<RowData> {

  private final PollingClientFactory<RowData> pollingClientFactory;

  @Getter private final ColumnData columnData;

  @Getter private final HttpLookupConfig options;

  private transient AtomicInteger localHttpCallCounter;

  @Builder
  public HttpTableLookupFunction(
      PollingClientFactory<RowData> pollingClientFactory,
      ColumnData columnData,
      HttpLookupConfig options) {

    this.pollingClientFactory = pollingClientFactory;
    this.columnData = columnData;
    this.options = options;
  }

  private PollingClient<RowData> client;

  @Override
  public void open(FunctionContext context) throws Exception {
    super.open(context);
    this.localHttpCallCounter = new AtomicInteger(0);
    this.client = pollingClientFactory.createPollClient(context, options);
    Gauge<Integer> httpCallCounter = context
        .getMetricGroup()
        .gauge("http-table-lookup-call-counter", () -> localHttpCallCounter.intValue());
  }

  /** This is a lookup method which is called by Flink framework in a runtime. */
  public void eval(Object... keys) {
    RowData result = lookupByKeys(keys);
    collect(result);
  }

  public RowData lookupByKeys(Object[] keys) {
    RowData keyRow = GenericRowData.of(keys);
    log.debug("Used Keys - {}", keyRow);

    // TODO Implement transient Cache here
    List<LookupArg> lookupArgs = new ArrayList<>(keys.length);
    for (int i = 0; i < keys.length; i++) {
      LookupArg lookupArg = processKey(columnData.getKeyNames()[i], keys[i]);
      lookupArgs.add(lookupArg);
    }

    localHttpCallCounter.incrementAndGet();
    return client.pull(lookupArgs);
  }

  // TODO implement all Flink Types here.
  private LookupArg processKey(String keyName, Object key) {
    String keyValue;

    if (!(key instanceof BinaryStringData)) {
      log.warn(
          "Unsupported Key Type {}. Trying simple toString(), wish me luck...", key.getClass());
    }
    keyValue = key.toString();

    return new LookupArg(keyName, keyValue);
  }

  @Data
  @Builder
  @RequiredArgsConstructor(access = AccessLevel.PRIVATE)
  public static class ColumnData implements Serializable {
    private final String[] keyNames;
  }
}
