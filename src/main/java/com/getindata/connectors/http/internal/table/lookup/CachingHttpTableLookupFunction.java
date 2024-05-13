package com.getindata.connectors.http.internal.table.lookup;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.table.connector.source.lookup.cache.LookupCache;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.LookupFunction;

import com.getindata.connectors.http.internal.PollingClient;
import com.getindata.connectors.http.internal.PollingClientFactory;
import com.getindata.connectors.http.internal.utils.SerializationSchemaUtils;

@Slf4j
public class CachingHttpTableLookupFunction extends LookupFunction {
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

    private LookupCache cache;

    public CachingHttpTableLookupFunction(
            PollingClientFactory<RowData> pollingClientFactory,
            DeserializationSchema<RowData> responseSchemaDecoder,
            LookupRow lookupRow,
            HttpLookupConfig options,
            LookupCache cache) {

        this.pollingClientFactory = pollingClientFactory;
        this.responseSchemaDecoder = responseSchemaDecoder;
        this.lookupRow = lookupRow;
        this.options = options;
        this.cache = cache;
    }

    @Override
    public void open(FunctionContext context) throws Exception {
        super.open(context);

        this.responseSchemaDecoder.open(
            SerializationSchemaUtils
                .createDeserializationInitContext(CachingHttpTableLookupFunction.class));

        this.localHttpCallCounter = new AtomicInteger(0);
        this.client = pollingClientFactory
            .createPollClient(options, responseSchemaDecoder);

        context
            .getMetricGroup()
            .gauge("http-table-lookup-call-counter", () -> localHttpCallCounter.intValue());
    }

    /**
     * This is a lookup method which is called by Flink framework at runtime.
     */
    @Override
    public Collection<RowData> lookup(RowData keyRow) throws IOException {
        log.debug("lookup=" + lookupRow);
        localHttpCallCounter.incrementAndGet();
        Optional<RowData> rowData=  client.pull(keyRow);
        List<RowData> result = new ArrayList<>();
        rowData.ifPresent(row -> { result.add(row); });
        log.debug("lookup result=" + result);
        return result;
    }
}
