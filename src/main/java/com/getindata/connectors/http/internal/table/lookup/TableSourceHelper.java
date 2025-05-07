package com.getindata.connectors.http.internal.table.lookup;

import java.util.Collections;
import java.util.List;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks;
import static org.apache.flink.table.types.logical.utils.LogicalTypeChecks.isCompositeType;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class TableSourceHelper {

    /**
     * Returns the first-level field names for the provided {@link DataType}.
     *
     * <p>Note: This method returns an empty list for every {@link DataType} that is not a
     * composite
     * type.
     * @param type logical type
     * @return List of field names
     */
    public static List<String> getFieldNames(LogicalType type) {

        if (type.getTypeRoot() == LogicalTypeRoot.DISTINCT_TYPE) {
            return getFieldNames(type.getChildren().get(0));
        } else if (isCompositeType(type)) {
            return LogicalTypeChecks.getFieldNames(type);
        }
        return Collections.emptyList();
    }

    /**
     * Builds {@link RowData} object based on provided list of values.
     * @param values values to use as {@link RowData} column values.
     * @return new {@link RowData} instance.
     */
    public static RowData buildGenericRowData(List<Object> values) {
        GenericRowData row = new GenericRowData(values.size());

        for (int i = 0; i < values.size(); ++i) {
            row.setField(i, values.get(i));
        }

        return row;
    }
}
