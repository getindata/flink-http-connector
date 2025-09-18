package com.getindata.connectors.http.internal.table.lookup;

import java.util.List;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.DataType;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.fail;

public class HttpTableLookupFunctionTest {
    private static @NotNull HttpTableLookupFunction getHttpTableLookupFunction(Boolean flag) {
        // Create metadata converters for testing
        MetadataConverter[] metadataConverters = new MetadataConverter[] {
            // Simple converter that returns a string
            new MetadataConverter() {
                private static final long serialVersionUID = 1L;
                @Override
                public Object read(String msg, HttpRowDataWrapper httpRowDataWrapper) {
                    return StringData.fromString(msg != null ? msg : "");
                }
            }
        };

        // Create a produced data type with one physical field and one metadata field
        DataType producedDataType = DataTypes.ROW(List.of(
            DataTypes.FIELD("id", DataTypes.STRING().notNull()),
            DataTypes.FIELD("error", DataTypes.STRING())
        ));

        HttpTableLookupFunction httpTableLookupFunction
                = new HttpTableLookupFunction(null,
                null,
                null,
                null,
                metadataConverters,
                producedDataType);
        return httpTableLookupFunction;
    }
    @Test

    void testprocessExceptionFromLookup() {
        // Create test instances with different fail_job_on_error settings
        HttpTableLookupFunction lookupFunctionWithFail = getHttpTableLookupFunction(true);
        HttpTableLookupFunction lookupFunctionWithoutFail = getHttpTableLookupFunction(false);

        // Create test exceptions
        RuntimeException runtimeException = new RuntimeException("test1");

        // No metadata so expect an Exception
        try {
            lookupFunctionWithFail.processExceptionFromLookup(runtimeException, 0);
            fail("Expected RuntimeException to be thrown");
        } catch (RuntimeException e) {
            // Expected exception
            assertThat(e).isSameAs(runtimeException);
        }

        // metadata so expect 1 row data
        List<RowData> rowData1 = lookupFunctionWithoutFail.processExceptionFromLookup(runtimeException, 1);
        assertThat(rowData1).isNotNull();
        assertThat(rowData1).hasSize(1);
        // metadata so expect 10 row data
        rowData1 = lookupFunctionWithoutFail.processExceptionFromLookup(runtimeException, 10);
        assertThat(rowData1).isNotNull();
        assertThat(rowData1).hasSize(10);

    }

}
