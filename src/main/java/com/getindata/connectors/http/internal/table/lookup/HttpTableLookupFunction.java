package com.getindata.connectors.http.internal.table.lookup;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.TableFunction;

import com.getindata.connectors.http.internal.PollingClient;
import com.getindata.connectors.http.internal.PollingClientFactory;
import com.getindata.connectors.http.internal.utils.SerializationSchemaUtils;

@Slf4j
public class HttpTableLookupFunction extends TableFunction<RowData> {

    private final PollingClientFactory<RowData> pollingClientFactory;

    private final DeserializationSchema<RowData> responseSchemaDecoder;

    @VisibleForTesting
    @Getter(AccessLevel.PACKAGE)
    private final LookupRow lookupRow;

    @VisibleForTesting
    @Getter(AccessLevel.PACKAGE)
    private final HttpLookupConfig options;

    private transient AtomicInteger localHttpCallCounter;

    private transient PollingClient<RowData> client;

    public HttpTableLookupFunction(
            PollingClientFactory<RowData> pollingClientFactory,
            DeserializationSchema<RowData> responseSchemaDecoder,
            LookupRow lookupRow,
            HttpLookupConfig options) {

        this.pollingClientFactory = pollingClientFactory;
        this.responseSchemaDecoder = responseSchemaDecoder;
        this.lookupRow = lookupRow;
        this.options = options;
    }

    @Override
    public void open(FunctionContext context) throws Exception {
        super.open(context);

        this.responseSchemaDecoder.open(
            SerializationSchemaUtils
                .createDeserializationInitContext(HttpTableLookupFunction.class));

        this.localHttpCallCounter = new AtomicInteger(0);
        this.client = pollingClientFactory
            .createPollClient(options, responseSchemaDecoder);

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
        RowData rowData = GenericRowData.of(keys);
        localHttpCallCounter.incrementAndGet();
        return client.pull(rowData);
    }
}
