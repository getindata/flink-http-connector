package com.getindata.connectors.http.internal.table.lookup;

import java.util.List;
import java.util.Map;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.DataType;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.fail;

import com.getindata.connectors.http.HttpStatusCodeValidationFailedException;
import static com.getindata.connectors.http.internal.table.lookup.HttpLookupConnectorOptions.SOURCE_LOOKUP_FAIL_JOB_ON_ERROR;

public class HttpTableLookupFunctionTest {
    @Test
    void testconstructor() {
        assertThat(getHttpTableLookupFunction(null).getFail_job_on_error()).isTrue();
        assertThat(getHttpTableLookupFunction(true).getFail_job_on_error()).isTrue();
        assertThat(getHttpTableLookupFunction(false).getFail_job_on_error()).isFalse();
    }

    private static @NotNull HttpTableLookupFunction getHttpTableLookupFunction(Boolean flag) {
        ReadableConfig readableConfig;
        if (flag == null ) {
            readableConfig = new Configuration();
        } else {
            Map<String, String> optionsMap = Map.of(SOURCE_LOOKUP_FAIL_JOB_ON_ERROR.key(),
                    Boolean.valueOf(flag).toString());
            readableConfig=Configuration.fromMap(optionsMap);
        }
        HttpLookupConfig httpLookupConfig = HttpLookupConfig.builder()
                .readableConfig(readableConfig)
                .build();

        // Create metadata converters for testing
        MetadataConverter[] metadataConverters = new MetadataConverter[] {
            // Simple converter that returns a string
            new MetadataConverter() {
                private static final long serialVersionUID = 1L;
                @Override
                public Object read(String msg, java.net.http.HttpResponse httpResponse) {
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
                httpLookupConfig,
                metadataConverters,
                producedDataType);
        return httpTableLookupFunction;
    }
    @Test

    void testprocessExceptionsFromLookup() {
        // Create test instances with different fail_job_on_error settings
        HttpTableLookupFunction lookupFunctionWithFail = getHttpTableLookupFunction(true);
        HttpTableLookupFunction lookupFunctionWithoutFail = getHttpTableLookupFunction(false);

        // Create test exceptions
        RuntimeException runtimeException1 = new RuntimeException("test1");

        // Use null for the HttpResponse parameter
        HttpStatusCodeValidationFailedException cause =
            new HttpStatusCodeValidationFailedException("status", null);
        RuntimeException runtimeException2 = new RuntimeException("test2", cause);

        // Test case 1: When fail_job_on_error is true, it should throw the exception
        try {
            lookupFunctionWithFail.processExceptionsFromLookup(runtimeException1, 3);
            fail("Expected RuntimeException to be thrown");
        } catch (RuntimeException e) {
            // Expected exception
            assertThat(e).isSameAs(runtimeException1);
        }

        // Test case 2: When fail_job_on_error is false and the cause is not an HttpStatusCodeValidationFailedException
        List<RowData> rowData1 = lookupFunctionWithoutFail.processExceptionsFromLookup(runtimeException1, 1);
        assertThat(rowData1).isNotNull();
        assertThat(rowData1).hasSize(1);

        // Test case 3: When fail_job_on_error is false and the cause is an HttpStatusCodeValidationFailedException
        List<RowData> rowData2 = lookupFunctionWithoutFail.processExceptionsFromLookup(runtimeException2, 1);
        assertThat(rowData2).isNotNull();
        assertThat(rowData2).hasSize(1);
    }

}
