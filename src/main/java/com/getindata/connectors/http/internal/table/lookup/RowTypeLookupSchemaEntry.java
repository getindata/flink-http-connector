package com.getindata.connectors.http.internal.table.lookup;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.RowData.FieldGetter;

import com.getindata.connectors.http.LookupArg;

/**
 * Implementation of {@link LookupSchemaEntry} for {@link RowData} type that represents multiple
 * columns.
 */
public class RowTypeLookupSchemaEntry extends RowDataLookupSchemaEntryBase<RowData> {

    /**
     * {@link LookupSchemaEntry} elements for every lookup column represented by {@link
     * RowTypeLookupSchemaEntry} instance.
     */
    private final List<LookupSchemaEntry<RowData>> keyColumns;

    /**
     * Creates new instance.
     *
     * @param fieldName   field name that this instance represents, matching {@link RowData} column
     *                    name.
     * @param fieldGetter {@link RowData.FieldGetter} for data type matching {@link RowData} column
     *                    type that this instance represents.
     */
    public RowTypeLookupSchemaEntry(String fieldName, FieldGetter fieldGetter) {
        super(fieldName, fieldGetter);
        this.keyColumns = new LinkedList<>();
    }

    /**
     * Add {@link LookupSchemaEntry} to keyColumns that this {@link RowTypeLookupSchemaEntry}
     * represents.
     *
     * @param lookupSchemaEntry {@link LookupSchemaEntry} to add.
     * @return this {@link RowTypeLookupSchemaEntry} instance.
     */
    public RowTypeLookupSchemaEntry addLookupEntry(LookupSchemaEntry<RowData> lookupSchemaEntry) {
        this.keyColumns.add(lookupSchemaEntry);
        return this;
    }

    /**
     * Creates collection of {@link LookupArg} that represents every lookup element from {@link
     * LookupSchemaEntry} added to this instance.
     *
     * @param lookupKeyRow Element to get the values from for {@code LookupArg#getArgValue()}.
     * @return collection of {@link LookupArg} that represents entire lookup {@link RowData}
     */
    @Override
    public List<LookupArg> convertToLookupArg(RowData lookupKeyRow) {

        RowData nestedRow = (RowData) fieldGetter.getFieldOrNull(lookupKeyRow);

        if (nestedRow == null) {
            return Collections.emptyList();
        }

        List<LookupArg> lookupArgs = new LinkedList<>();
        for (LookupSchemaEntry<RowData> lookupSchemaEntry : keyColumns) {
            lookupArgs.addAll(lookupSchemaEntry.convertToLookupArg(nestedRow));
        }

        return lookupArgs;
    }

    @lombok.Generated
    @Override
    public String toString() {
        return "RowTypeLookupSchemaEntry{" +
            "fieldName='" + fieldName + '\'' +
            ", fieldGetter=" + fieldGetter +
            ", keyColumns=" + keyColumns +
            '}';
    }
}
