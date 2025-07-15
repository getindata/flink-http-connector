package com.getindata.connectors.http.internal.table.lookup;
import java.io.Serializable;
import java.net.http.HttpResponse;

interface MetadataConverter extends Serializable {
    Object read(String msg, HttpResponse httpResponse);
}
