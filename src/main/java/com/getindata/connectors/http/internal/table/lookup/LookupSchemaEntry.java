package com.getindata.connectors.http.internal.table.lookup;

import java.io.Serializable;
import java.util.List;

import com.getindata.connectors.http.LookupArg;

/**
 * Represents Lookup entry with its name and provides conversion method to collection of {@link
 * LookupArg} elements.
 *
 * @param <T> type of lookupKeyRow used for converting to {@link LookupArg}.
 */
public interface LookupSchemaEntry<T> extends Serializable {

    /**
     * @return lookup Field name.
     */
    String getFieldName();

    /**
     * Creates a collection of {@link LookupArg} elements from provided T lookupKeyRow
     *
     * @param lookupKeyRow Element to get the values from for {@code LookupArg#getArgValue()}.
     * @return Collection of {@link LookupArg} objects created from lookupKeyRow.
     */
    List<LookupArg> convertToLookupArg(T lookupKeyRow);
}
