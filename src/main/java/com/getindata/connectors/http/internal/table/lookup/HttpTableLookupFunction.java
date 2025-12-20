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
    private final DataType producedDataType;
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

        HttpRowDataWrapper httpRowDataWrapper = client.pull(keyRow);
        Collection<RowData> httpCollector = httpRowDataWrapper.getData();

        int physicalArity = -1;

        GenericRowData producedRow = null;
        // grab the actual data if there is any from the response and populate the producedRow with it
        if (!httpCollector.isEmpty()) {
            // TODO original code increments again if empty - removing
            //  if (httpCollector.isEmpty()) {
            // localHttpCallCounter.incrementAndGet();
            //} else {

            GenericRowData physicalRow = (GenericRowData) httpCollector.iterator().next();
            physicalArity = physicalRow.getArity();
            producedRow = new GenericRowData(physicalRow.getRowKind(), physicalArity + metadataArity);
            // We need to copy in the physical row into the producedRow
            for (int pos = 0; pos < physicalArity; pos++) {
                producedRow.setField(pos, physicalRow.getField(pos));
            }
        }
        // if we did not get the physical arity from the http response physical row then get it from the
        // producedDataType. which is set when we have metadata or when there's no data
        if (physicalArity == -1) {
            if (producedDataType != null) {
                List<LogicalType> childrenLogicalTypes = producedDataType.getLogicalType().getChildren();
                physicalArity = childrenLogicalTypes.size() - metadataArity;
            } else {
                // If producedDataType is null and we have no data, return the same way as ignore.
                return Collections.emptyList();
            }
        }
        // if there was no data, create an empty producedRow
        if (producedRow == null) {
            producedRow = new GenericRowData(RowKind.INSERT, physicalArity + metadataArity);
        }
        // add any metadata
        if (producedDataType != null ) {
            for (int metadataPos = 0; metadataPos < metadataArity; metadataPos++) {
                producedRow.setField(
                        physicalArity + metadataPos,
                        metadataConverters[metadataPos].read(httpRowDataWrapper));
            }
        }
        outputList.add(producedRow);
        return outputList;
    }
}

