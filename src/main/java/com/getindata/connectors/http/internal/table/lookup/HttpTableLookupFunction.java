package com.getindata.connectors.http.internal.table.lookup;

import java.net.http.HttpResponse;
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

import com.getindata.connectors.http.HttpStatusCodeValidationFailedException;
import com.getindata.connectors.http.internal.PollingClient;
import com.getindata.connectors.http.internal.PollingClientFactory;
import com.getindata.connectors.http.internal.utils.SerializationSchemaUtils;
import static com.getindata.connectors.http.internal.table.lookup.HttpLookupConnectorOptions.SOURCE_LOOKUP_FAIL_JOB_ON_ERROR;

@Slf4j
public class HttpTableLookupFunction extends LookupFunction {

    private final PollingClientFactory<RowData> pollingClientFactory;

    private final DeserializationSchema<RowData> responseSchemaDecoder;

    @VisibleForTesting
    @Getter(AccessLevel.PACKAGE)
    private final LookupRow lookupRow;

    @VisibleForTesting
    @Getter(AccessLevel.PACKAGE)
    private final HttpLookupConfig options;
    private final  DataType producedDataType;
    private final Boolean fail_job_on_error;
    private transient AtomicInteger localHttpCallCounter;
    private transient PollingClient<RowData> client;
    private final MetadataConverter[] metadataConverters;
    public HttpTableLookupFunction(
            PollingClientFactory<RowData> pollingClientFactory,
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
        Optional<Boolean> optional = options.getReadableConfig().getOptional(SOURCE_LOOKUP_FAIL_JOB_ON_ERROR);
        this.fail_job_on_error = optional.orElse(true);
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
            Collection<RowData> httpCollector = client.pull(keyRow);
            // When there are no metadata columns or we have no results then
            // return what the client pulls.
            if (this.fail_job_on_error || httpCollector.size() == 0) {
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
        } catch (Exception e) {
            if (this.fail_job_on_error) throw e;
            Throwable cause =e.getCause();
            if (cause instanceof HttpStatusCodeValidationFailedException) {
                final GenericRowData producedRow = getProducedRowForError(
                        cause.getMessage(),
                        ((HttpStatusCodeValidationFailedException) cause).getResponse(),
                        metadataArity,
                        metadataConverters);
                outputList.add(producedRow);
            } else {
                // cause might not have a message so just use it's toString.
                String msg= e.getMessage() +"," + cause;
                final GenericRowData producedRow = getProducedRowForError(
                        msg,
                        null,
                        metadataArity,
                        metadataConverters);
                outputList.add(producedRow);
            }
        }
        return outputList;
    }

    private GenericRowData getProducedRowForError(String msg,
                                                  HttpResponse httpResponse,
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
                    metadataConverters[metadataPos].read(msg, httpResponse));
        }
        return producedRow;
    }
    @VisibleForTesting
    Boolean getFail_job_on_error() {
        return fail_job_on_error;
    }
}
