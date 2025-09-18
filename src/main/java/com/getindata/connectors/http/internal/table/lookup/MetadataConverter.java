package com.getindata.connectors.http.internal.table.lookup;
import java.io.Serializable;

interface MetadataConverter extends Serializable {
    Object read(String msg, HttpRowDataWrapper httpRowDataWrapper);
}
