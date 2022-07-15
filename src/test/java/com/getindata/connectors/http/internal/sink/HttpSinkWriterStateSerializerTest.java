package com.getindata.connectors.http.internal.sink;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import org.apache.flink.connector.base.sink.writer.BufferedRequestState;
import org.apache.flink.connector.base.sink.writer.ElementConverter;
import org.junit.jupiter.api.Test;
import static org.apache.flink.connector.base.sink.writer.AsyncSinkWriterTestUtils.assertThatBufferStatesAreEqual;
import static org.apache.flink.connector.base.sink.writer.AsyncSinkWriterTestUtils.getTestState;

public class HttpSinkWriterStateSerializerTest {

    private static final ElementConverter<String, HttpSinkRequestEntry> ELEMENT_CONVERTER =
        (s, _context) ->
            new HttpSinkRequestEntry("POST", s.getBytes(StandardCharsets.UTF_8));

    @Test
    public void testSerializeAndDeserialize() throws IOException {
        BufferedRequestState<HttpSinkRequestEntry> expectedState =
            getTestState(ELEMENT_CONVERTER,
                httpSinkRequestEntry -> Math.toIntExact(httpSinkRequestEntry.getSizeInBytes()));

        HttpSinkWriterStateSerializer serializer = new HttpSinkWriterStateSerializer();
        BufferedRequestState<HttpSinkRequestEntry> actualState =
            serializer.deserialize(1, serializer.serialize(expectedState));

        assertThatBufferStatesAreEqual(actualState, expectedState);
    }
}
