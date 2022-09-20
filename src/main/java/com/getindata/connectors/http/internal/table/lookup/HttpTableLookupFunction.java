package com.getindata.connectors.http.internal.table.lookup;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import lombok.AccessLevel;
import lombok.Builder;
import lombok.Data;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryStringData;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.TableFunction;

import com.getindata.connectors.http.LookupArg;
import com.getindata.connectors.http.LookupQueryCreator;
import com.getindata.connectors.http.internal.PollingClient;
import com.getindata.connectors.http.internal.PollingClientFactory;

@Slf4j
public class HttpTableLookupFunction extends TableFunction<RowData> {

    private final PollingClientFactory<RowData> pollingClientFactory;

    private final DeserializationSchema<RowData> schemaDecoder;

    private final LookupQueryCreator lookupQueryCreator;

    @Getter
    private final ColumnData columnData;

    @Getter
    private final HttpLookupConfig options;

    private transient AtomicInteger localHttpCallCounter;

    private transient PollingClient<RowData> client;

    @Builder
    private HttpTableLookupFunction(
        PollingClientFactory<RowData> pollingClientFactory,
        DeserializationSchema<RowData> schemaDecoder,
        ColumnData columnData,
        HttpLookupConfig options,
        LookupQueryCreator lookupQueryCreator) {

        this.pollingClientFactory = pollingClientFactory;
        this.schemaDecoder = schemaDecoder;
        this.columnData = columnData;
        this.options = options;
        this.lookupQueryCreator = lookupQueryCreator;
    }

    @Override
    public void open(FunctionContext context) throws Exception {
        super.open(context);
        this.localHttpCallCounter = new AtomicInteger(0);
        this.client = pollingClientFactory
            .createPollClient(options, schemaDecoder, lookupQueryCreator);

        context
            .getMetricGroup()
            .gauge("http-table-lookup-call-counter", () -> localHttpCallCounter.intValue());
    }

    /**
     * This is a lookup method which is called by Flink framework in a runtime.
     */
    public void eval(Object... keys) {
        lookupByKeys(keys)
            .ifPresent(this::collect);
    }

    public Optional<RowData> lookupByKeys(Object[] keys) {
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
                "Unsupported Key Type {}. Trying simple toString(), wish me luck...",
                key.getClass());
        }
        keyValue = key.toString();

        return new LookupArg(keyName, keyValue);
    }

    // TODOESP-148 DO I need this??
    @Data
    @Builder
    @RequiredArgsConstructor(access = AccessLevel.PRIVATE)
    public static class ColumnData implements Serializable {

        private final String[] keyNames;
    }
}
