package com.getindata.connectors.http.internal.table;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.connector.sink2.Sink.InitContext;
import org.apache.flink.api.connector.sink2.SinkWriter.Context;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.FlinkRuntimeException;

import com.getindata.connectors.http.SchemaLifecycleAwareElementConverter;
import com.getindata.connectors.http.internal.sink.HttpSinkRequestEntry;

public class SerializationSchemaElementConverter
    implements SchemaLifecycleAwareElementConverter<RowData, HttpSinkRequestEntry> {

    private final String insertMethod;

    private final SerializationSchema<RowData> serializationSchema;

    private boolean schemaOpened = false;

    public SerializationSchemaElementConverter(
        String insertMethod,
        SerializationSchema<RowData> serializationSchema) {

        this.insertMethod = insertMethod;
        this.serializationSchema = serializationSchema;
    }

    @Override
    public void open(InitContext context) {
        if (!schemaOpened) {
            try {
                serializationSchema.open(context.asSerializationSchemaInitializationContext());
                schemaOpened = true;
            } catch (Exception e) {
                throw new FlinkRuntimeException("Failed to initialize serialization schema.", e);
            }
        }
    }

    @Override
    public HttpSinkRequestEntry apply(RowData rowData, Context context) {
        return new HttpSinkRequestEntry(
            insertMethod,
            serializationSchema.serialize(rowData));
    }
}
