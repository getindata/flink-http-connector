package com.getindata.connectors.http.internal.table.lookup;
import java.io.Serializable;

/**
 * The metadata converters have a read method that is passed a HttpRowDataWrapper. The implementations
 * pick out the appropriate value of the metadata from this object.
 */
interface MetadataConverter extends Serializable {
    /**
     *
     * @param httpRowDataWrapper an object that contains all metadata content
     * @return the metadata value for this MetadataConverter.
     */
    Object read(HttpRowDataWrapper httpRowDataWrapper);
}
