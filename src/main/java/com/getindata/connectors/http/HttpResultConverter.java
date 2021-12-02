package com.getindata.connectors.http;

import java.io.Serializable;
import org.apache.flink.table.data.RowData;

public interface HttpResultConverter extends Serializable {

  RowData convert(String json);
}
