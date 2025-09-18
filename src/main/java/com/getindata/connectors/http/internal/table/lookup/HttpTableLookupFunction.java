package com.getindata.connectors.http.internal.table.lookup;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.LookupFunction;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.types.RowKind;

import com.getindata.connectors.http.internal.PollingClient;
import com.getindata.connectors.http.internal.PollingClientFactory;
import com.getindata.connectors.http.internal.utils.SerializationSchemaUtils;

@Slf4j
public class HttpTableLookupFunction extends LookupFunction {

    private final PollingClientFactory pollingClientFactory;

    private final DeserializationSchema<RowData> responseSchemaDecoder;

    @VisibleForTesting
    @Getter(AccessLevel.PACKAGE)
    private final LookupRow lookupRow;

    @VisibleForTesting
    @Getter(AccessLevel.PACKAGE)
    private final HttpLookupConfig options;
    private final  DataType producedDataType;
    private transient AtomicInteger localHttpCallCounter;
    private transient PollingClient client;
    private final MetadataConverter[] metadataConverters;
    public HttpTableLookupFunction(
            PollingClientFactory pollingClientFactory,
            DeserializationSchema<RowData> responseSchemaDecoder,
            LookupRow lookupRow,
            HttpLookupConfig options,
            MetadataConverter[] metadataConverters,
            DataType producedDataType
    ) {

        this.pollingClientFactory = pollingClientFactory;
        this.responseSchemaDecoder = responseSchemaDecoder;
        this.lookupRow = lookupRow;
        this.options = options;
        this.metadataConverters = metadataConverters;
        this.producedDataType = producedDataType;
    }

    @Override
    public void open(FunctionContext context) throws Exception {
        this.responseSchemaDecoder.open(
            SerializationSchemaUtils
                .createDeserializationInitContext(HttpTableLookupFunction.class));

        this.localHttpCallCounter = new AtomicInteger(0);
        this.client = pollingClientFactory
                .createPollClient(options, responseSchemaDecoder);

        context.getMetricGroup()
                .gauge("http-table-lookup-call-counter", () -> localHttpCallCounter.intValue());

        client.open(context);
    }

    @Override
    public Collection<RowData> lookup(RowData keyRow) {
        localHttpCallCounter.incrementAndGet();
        List<RowData> outputList = new ArrayList<>();
        final int metadataArity = metadataConverters.length;
        try {
            HttpRowDataWrapper httpRowDataWrapper = client.pull(keyRow);
            Collection<RowData> httpCollector = httpRowDataWrapper.getData();
            // When  we have no results then return what the client pulls.
            if (httpCollector.size() == 0) {
                localHttpCallCounter.incrementAndGet();
                return httpCollector;
            } else {
                GenericRowData physicalRow = (GenericRowData) httpCollector.iterator().next();
                final int physicalArity = physicalRow.getArity();
                final GenericRowData producedRow =
                    new GenericRowData(physicalRow.getRowKind(), physicalArity + metadataArity);
                // We need to copy in the physical row into the producedRow
                for (int pos = 0; pos < physicalArity; pos++) {
                    producedRow.setField(pos, physicalRow.getField(pos));
                }
                outputList.add(producedRow);
            }
        } catch (RuntimeException e) {
            outputList = processExceptionFromLookup(e, metadataArity);
        }
        return outputList;
    }

    /**
     * There was an Exception thrown as part of the http processing. If there is no metadata, throw the exception,
     * if there is the error message metadata column defined then populate it with the Exception Message
     * we can add into the metadata
     * @param e Exception from http lookup
     * @param metadataArity metadata arity
     * @return Row data list including any metadata columns
     */
    List<RowData> processExceptionFromLookup(RuntimeException e, int metadataArity) {
        List<RowData> outputList = new ArrayList<>();
        if (this.metadataConverters.length == 0 ) throw e;
        String msg = e.getMessage() + "," + e.getCause();
        RowData producedRow = getProducedRowForError(msg, metadataArity, metadataConverters);
        outputList.add(producedRow);
        return outputList;
    }

    @VisibleForTesting
    private GenericRowData getProducedRowForError(String msg,
                                                  int metadataArity,
                                                  MetadataConverter[] metadataConverters) {
        List<LogicalType> childrenLogicalTypes=producedDataType.getLogicalType().getChildren();
        final GenericRowData producedRow =
                new GenericRowData(RowKind.INSERT, childrenLogicalTypes.size());
        int physicalArity=childrenLogicalTypes.size()-metadataArity;
        // by not specifying the physical field values we get:
        // for physical nullable fields, the values are null
        // for physical non nullable fields the values are the "default" for a type
        for (int metadataPos = 0; metadataPos < metadataArity; metadataPos++) {
            producedRow.setField(
                    physicalArity + metadataPos,
                    metadataConverters[metadataPos].read(msg, null));
        }
        return producedRow;
    }
}
