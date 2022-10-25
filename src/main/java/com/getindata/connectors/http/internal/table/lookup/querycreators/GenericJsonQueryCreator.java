package com.getindata.connectors.http.internal.table.lookup.querycreators;

import java.nio.charset.StandardCharsets;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.table.data.RowData;

import com.getindata.connectors.http.LookupQueryCreator;

/**
 * A {@link LookupQueryCreator} that builds Json based body for REST requests, i.e. adds
 */
public class GenericJsonQueryCreator implements LookupQueryCreator {

    /**
     * The {@link SerializationSchema} to serialize {@link RowData} object.
     */
    private final SerializationSchema<RowData> jsonSerialization;

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
        return new String(jsonSerialization.serialize(lookupDataRow), StandardCharsets.UTF_8);
    }
}
