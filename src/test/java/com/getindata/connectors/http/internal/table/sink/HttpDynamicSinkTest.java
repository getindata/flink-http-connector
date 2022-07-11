package com.getindata.connectors.http.internal.table.sink;

import java.util.Map;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.TestFormatFactory;
import org.apache.flink.table.types.AtomicDataType;
import org.apache.flink.table.types.logical.BooleanType;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import static com.getindata.connectors.http.internal.table.sink.HttpDynamicSinkConnectorOptions.INSERT_METHOD;
import static com.getindata.connectors.http.internal.table.sink.HttpDynamicSinkConnectorOptions.URL;

public class HttpDynamicSinkTest {

    @Test
    public void copyEqualityTest() {
        var mockFormat = new TestFormatFactory.EncodingFormatMock(",", ChangelogMode.insertOnly());
        var sink = new HttpDynamicSink
            .HttpDynamicTableSinkBuilder()
            .setTableOptions(
                new Configuration() {
                    {
                        this.set(URL, "localhost:8123");
                        this.set(INSERT_METHOD, "POST");
                        this.set(FactoryUtil.FORMAT, "json");
                    }
                }
            )
            .setConsumedDataType(
                new AtomicDataType(new BooleanType(false)))
            .setEncodingFormat(mockFormat)
            .setFormatContentTypeMap(Map.of())
            .build();

        assertEquals(sink, sink.copy());
        assertEquals(sink.hashCode(), sink.copy().hashCode());
    }

    private HttpDynamicSink.HttpDynamicTableSinkBuilder getSinkBuilder() {
        var mockFormat = new TestFormatFactory.EncodingFormatMock(",", ChangelogMode.insertOnly());
        var consumedDataType = new AtomicDataType(new BooleanType(false));

        return new HttpDynamicSink.HttpDynamicTableSinkBuilder()
            .setTableOptions(
                new Configuration() {
                    {
                        this.set(URL, "localhost:8123");
                        this.set(INSERT_METHOD, "POST");
                        this.set(FactoryUtil.FORMAT, "json");
                    }
                }
            )
            .setConsumedDataType(consumedDataType)
            .setEncodingFormat(mockFormat)
            .setFormatContentTypeMap(Map.of())
            .setMaxBatchSize(1);
    }

    @Test
    public void nonEqualsTest() {
        var sink = getSinkBuilder().build();
        var sinkBatchSize = getSinkBuilder().setMaxBatchSize(10).build();
        var sinkSinkConfig = getSinkBuilder().setTableOptions(
            new Configuration() {
                {
                    this.set(URL, "localhost:8124");
                    this.set(INSERT_METHOD, "POST");
                    this.set(FactoryUtil.FORMAT, "json");
                }
            }
        ).build();
        var sinkDataType =
            getSinkBuilder().setConsumedDataType(new AtomicDataType(new BooleanType(true))).build();
        var sinkFormat = getSinkBuilder().setEncodingFormat(
            new TestFormatFactory.EncodingFormatMock(";", ChangelogMode.all())).build();

        assertEquals(sink, sink);
        assertNotEquals(null, sink);
        assertNotEquals("test-string", sink);
        assertNotEquals(sink, sinkBatchSize);
        assertNotEquals(sink, sinkSinkConfig);
        assertNotEquals(sink, sinkDataType);
        assertNotEquals(sink, sinkFormat);
    }
}
