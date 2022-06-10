package com.getindata.connectors.http.table;

import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.factories.TestFormatFactory;
import org.apache.flink.table.types.AtomicDataType;
import org.apache.flink.table.types.logical.BooleanType;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class HttpDynamicSinkTest {
  @Test
  public void copyEqualityTest() {
    var mockFormat = new TestFormatFactory.EncodingFormatMock(",", ChangelogMode.insertOnly());
    var sink = new HttpDynamicSink
        .HttpDynamicTableSinkBuilder()
        .setSinkConfig(
            HttpDynamicSinkConfig
                .builder()
                .url("localhost:8123")
                .format("json")
                .insertMethod("POST")
                .build()
        )
        .setConsumedDataType(
            new AtomicDataType(new BooleanType(false)))
        .setEncodingFormat(mockFormat)
        .build();

    assertEquals(sink, sink.copy());
    assertEquals(sink.hashCode(), sink.copy().hashCode());
  }

  private HttpDynamicSink.HttpDynamicTableSinkBuilder getSinkBuilder() {
    var mockFormat = new TestFormatFactory.EncodingFormatMock(",", ChangelogMode.insertOnly());
    var consumedDataType = new AtomicDataType(new BooleanType(false));

    return new HttpDynamicSink.HttpDynamicTableSinkBuilder()
        .setSinkConfig(
            HttpDynamicSinkConfig
                .builder()
                .url("localhost:8123")
                .format("json")
                .insertMethod("POST")
                .build()
        )
        .setConsumedDataType(consumedDataType)
        .setEncodingFormat(mockFormat)
        .setMaxBatchSize(1);
  }

  @Test
  public void nonEqualsTest() {
    var sink = getSinkBuilder().build();
    var sinkBatchSize = getSinkBuilder().setMaxBatchSize(10).build();
    var sinkSinkConfig = getSinkBuilder().setSinkConfig(
        HttpDynamicSinkConfig
            .builder()
            .url("localhost:8124")
            .format("json")
            .insertMethod("POST")
            .build()
    ).build();
    var sinkDataType = getSinkBuilder().setConsumedDataType(new AtomicDataType(new BooleanType(true))).build();
    var sinkFormat = getSinkBuilder().setEncodingFormat(new TestFormatFactory.EncodingFormatMock(";", ChangelogMode.all())).build();

    assertEquals(sink, sink);
    assertFalse(sink.equals(null));
    assertFalse(sink.equals("test-string"));
    assertNotEquals(sink, sinkBatchSize);
    assertNotEquals(sink, sinkSinkConfig);
    assertNotEquals(sink, sinkDataType);
    assertNotEquals(sink, sinkFormat);
  }
}
