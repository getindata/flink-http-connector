package com.getindata.connectors.http.internal.table.lookup.querycreators;

import java.nio.charset.StandardCharsets;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.FlinkRuntimeException;

import com.getindata.connectors.http.LookupQueryCreator;
import com.getindata.connectors.http.internal.utils.SerializationSchemaUtils;

/**
 * A {@link LookupQueryCreator} that builds Json based body for REST requests, i.e. adds
 */
public class GenericJsonQueryCreator implements LookupQueryCreator {

    /**
     * The {@link SerializationSchema} to serialize {@link RowData} object.
     */
    private final SerializationSchema<RowData> jsonSerialization;

    private boolean schemaOpened = false;

    public GenericJsonQueryCreator(SerializationSchema<RowData> jsonSerialization) {

        this.jsonSerialization = jsonSerialization;
    }

    /**
     * Creates a Jason string from given {@link RowData}.
     *
     * @param lookupDataRow {@link RowData} to serialize into Json string.
     * @return Json string created from lookupDataRow argument.
     */
    @Override
    public String createLookupQuery(RowData lookupDataRow) {
        checkOpened();
        return new String(jsonSerialization.serialize(lookupDataRow), StandardCharsets.UTF_8);
    }

    private void checkOpened() {
        if (!schemaOpened) {
            try {
                jsonSerialization.open(
                    SerializationSchemaUtils
                        .createSerializationInitContext(GenericJsonQueryCreator.class));
            } catch (Exception e) {
                throw new FlinkRuntimeException(
                    "Failed to initialize serialization schema for GenericJsonQueryCreatorFactory.",
                    e);
            }
            schemaOpened = true;
        }
    }
}
