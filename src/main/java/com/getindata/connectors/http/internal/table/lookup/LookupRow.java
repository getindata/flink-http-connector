package com.getindata.connectors.http.internal.table.lookup;

import java.io.Serializable;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;

import com.getindata.connectors.http.LookupArg;

@ToString
public class LookupRow implements Serializable {

    private final List<LookupSchemaEntry<RowData>> lookupEntries;

    @Getter
    @Setter
    private DataType lookupPhysicalRowDataType;

    public LookupRow() {
        this.lookupEntries = new LinkedList<>();
    }

    /**
     * Creates a collection of {@link LookupArg} elements. Every column and its value from provided
     * {@link RowData} is converted to {@link LookupArg}.
     *
     * @param lookupDataRow A {@link RowData} to get the values from for {@code
     *                      LookupArg#getArgValue()}.
     * @return Collection of {@link LookupArg} objects created from lookupDataRow.
     */
    public Collection<LookupArg> convertToLookupArgs(RowData lookupDataRow) {
        List<LookupArg> lookupArgs = new LinkedList<>();
        for (LookupSchemaEntry<RowData> lookupSchemaEntry : lookupEntries) {
            lookupArgs.addAll(lookupSchemaEntry.convertToLookupArg(lookupDataRow));
        }
        return lookupArgs;
    }

    public LookupRow addLookupEntry(LookupSchemaEntry<RowData> lookupSchemaEntry) {
        this.lookupEntries.add(lookupSchemaEntry);
        return this;
    }

    @VisibleForTesting
    List<LookupSchemaEntry<RowData>> getLookupEntries() {
        return new LinkedList<>(lookupEntries);
    }
}
