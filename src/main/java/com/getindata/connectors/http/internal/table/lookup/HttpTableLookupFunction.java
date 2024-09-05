package com.getindata.connectors.http.internal.table.lookup;

import java.util.Collection;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.LookupFunction;

@Slf4j
public class HttpTableLookupFunction extends LookupFunction {

    private final HttpRequestFactory requestFactory;
    private final RawResponseBodyDecoder responseBodyDecoder;
    private final HttpTableLookupFunctionBase delegate;

    public HttpTableLookupFunction(HttpRequestFactory requestFactory,
                                   RawResponseBodyDecoder responseBodyDecoder,
                                   HttpTableLookupFunctionBase delegate) {
        this.requestFactory = requestFactory;
        this.responseBodyDecoder = responseBodyDecoder;
        this.delegate = delegate;
    }


    @Override
    public void open(FunctionContext context) throws Exception {
        this.responseBodyDecoder.open();
        this.delegate.open(context);
    }

    @Override
    public Collection<RowData> lookup(RowData keyRow) {
        HttpLookupSourceRequestEntry request = requestFactory.buildLookupRequest(keyRow);
        Collection<byte[]> rawResponse = delegate.lookup(request);
        return responseBodyDecoder.deserialize(rawResponse);
    }

    @VisibleForTesting
    public LookupRow getLookupRow() {
        return delegate.getLookupRow();
    }

    @VisibleForTesting
    public HttpLookupConfig getOptions() {
        return delegate.getOptions();
    }
}
