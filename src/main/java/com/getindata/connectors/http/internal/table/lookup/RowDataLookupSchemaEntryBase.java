package com.getindata.connectors.http.internal.table.lookup;

import org.apache.flink.table.data.RowData;

/**
 * Base implementation of {@link LookupSchemaEntry} for {@link RowData} type.
 */
public abstract class RowDataLookupSchemaEntryBase<T> implements LookupSchemaEntry<RowData> {

    /**
     * Lookup field name represented by this instance.
     */
    protected final String fieldName;

    /**
     * {@link RowData.FieldGetter} matching RowData type for field represented by this instance.
     */
    protected final RowData.FieldGetter fieldGetter;

    /**
     * Creates new instance.
     *
     * @param fieldName   field name that this instance represents, matching {@link RowData} column
     *                    name.
     * @param fieldGetter {@link RowData.FieldGetter} for data type matching {@link RowData} column
     *                    type that this instance represents.
     */
    public RowDataLookupSchemaEntryBase(String fieldName, RowData.FieldGetter fieldGetter) {
        this.fieldName = fieldName;
        this.fieldGetter = fieldGetter;
    }

    public String getFieldName() {
        return this.fieldName;
    }
}
