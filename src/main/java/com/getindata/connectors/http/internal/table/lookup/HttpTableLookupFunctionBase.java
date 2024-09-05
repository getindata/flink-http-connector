package com.getindata.connectors.http.internal.table.lookup;

import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.table.functions.FunctionContext;

import com.getindata.connectors.http.internal.PollingClient;
import com.getindata.connectors.http.internal.PollingClientFactory;

@Slf4j
public class HttpTableLookupFunctionBase implements Serializable {

    private final PollingClientFactory<byte[]> pollingClientFactory;

    @VisibleForTesting
    @Getter(AccessLevel.PACKAGE)
    private final LookupRow lookupRow;

    @VisibleForTesting
    @Getter(AccessLevel.PACKAGE)
    private final HttpLookupConfig options;

    private transient AtomicInteger localHttpCallCounter;

    private transient PollingClient<byte[]> client;

    public HttpTableLookupFunctionBase(
            PollingClientFactory<byte[]> pollingClientFactory,
            LookupRow lookupRow,
            HttpLookupConfig options) {

        this.pollingClientFactory = pollingClientFactory;
        this.lookupRow = lookupRow;
        this.options = options;
    }

    public void open(FunctionContext context) throws Exception {
        this.localHttpCallCounter = new AtomicInteger(0);
        this.client = pollingClientFactory.createPollClient(options);

        context.getMetricGroup()
                .gauge("http-table-lookup-call-counter", () -> localHttpCallCounter.intValue());
    }

    public Collection<byte[]> lookup(HttpLookupSourceRequestEntry request) {
        localHttpCallCounter.incrementAndGet();
        Optional<byte[]> result = client.pull(request);
        return result.map(Collections::singletonList).orElse(Collections.emptyList());
    }
}
