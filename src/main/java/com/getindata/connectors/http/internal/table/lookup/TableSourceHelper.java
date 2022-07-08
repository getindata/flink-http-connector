package com.getindata.connectors.http.internal.table.lookup;

import static org.apache.flink.table.types.logical.utils.LogicalTypeChecks.isCompositeType;

import java.util.Collections;
import java.util.List;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class TableSourceHelper {

  /**
   * Returns the first-level field names for the provided {@link DataType}.
   *
   * <p>Note: This method returns an empty list for every {@link DataType} that is not a composite
   * type.
   */
  public static List<String> getFieldNames(DataType dataType) {
    final LogicalType type = dataType.getLogicalType();

    if (type.getTypeRoot() == LogicalTypeRoot.DISTINCT_TYPE) {
      return getFieldNames(dataType.getChildren().get(0));
    } else if (isCompositeType(type)) {
      return LogicalTypeChecks.getFieldNames(type);
    }
    return Collections.emptyList();
  }

  public static GenericRowData buildGenericRowData(List<Object> values) {
    GenericRowData row = new GenericRowData(values.size());

    for (int i = 0; i < values.size(); ++i) {
      row.setField(i, values.get(i));
    }

    return row;
  }

  public static GenericRowData buildEmptyRow(int size) {
    return new GenericRowData(size);
  }
}
