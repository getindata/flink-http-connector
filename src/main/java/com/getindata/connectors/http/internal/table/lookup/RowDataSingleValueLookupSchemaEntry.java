package com.getindata.connectors.http.internal.table.lookup;

import java.util.Collections;
import java.util.List;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.RowData.FieldGetter;
import org.apache.flink.table.data.binary.BinaryStringData;

import com.getindata.connectors.http.LookupArg;

/**
 * Implementation of {@link LookupSchemaEntry} for {@link RowData} type that represents single
 * lookup column.
 */
@Slf4j
public class RowDataSingleValueLookupSchemaEntry extends RowDataLookupSchemaEntryBase<RowData> {

    /**
     * Creates new instance.
     *
     * @param fieldName   field name that this instance represents, matching {@link RowData} column
     *                    name.
     * @param fieldGetter {@link RowData.FieldGetter} for data type matching {@link RowData} column
     *                    type that this instance represents.
     */
    public RowDataSingleValueLookupSchemaEntry(String fieldName, FieldGetter fieldGetter) {
        super(fieldName, fieldGetter);
    }

    /**
     * Creates single element collection that contains {@link LookupArg} for single column from
     * given lookupKeyRow. The column is defined by 'fieldName' and 'fieldGetter' used for creating
     * {@link RowDataSingleValueLookupSchemaEntry} instance
     *
     * @param lookupKeyRow Element to get the values from for {@code LookupArg#getArgValue()}.
     * @return single element collection of {@link LookupArg}.
     */
    @Override
    public List<LookupArg> convertToLookupArg(RowData lookupKeyRow) {

        Object value = tryGetValue(lookupKeyRow);

        if (value == null) {
            return Collections.emptyList();
        }

        if (!(value instanceof BinaryStringData)) {
            log.debug("Unsupported Key Type {}. Trying simple toString().",
                value.getClass());
        }

        return Collections.singletonList(new LookupArg(getFieldName(), value.toString()));
    }

    private Object tryGetValue(RowData lookupKeyRow) {
        try {
            return fieldGetter.getFieldOrNull(lookupKeyRow);
        } catch (ClassCastException e) {
            throw new RuntimeException(
                "Class cast exception on field getter for field " + getFieldName(), e);
        }
    }

    @lombok.Generated
    @Override
    public String toString() {
        return "RowDataSingleValueLookupSchemaEntry{" +
            "fieldName='" + fieldName + '\'' +
            ", fieldGetter=" + fieldGetter +
            '}';
    }
}
