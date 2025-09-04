/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.http.sink;

import org.apache.flink.connector.base.sink.writer.BufferedRequestState;
import org.apache.flink.connector.base.sink.writer.ElementConverter;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static org.apache.flink.connector.base.sink.writer.AsyncSinkWriterTestUtils.assertThatBufferStatesAreEqual;
import static org.apache.flink.connector.base.sink.writer.AsyncSinkWriterTestUtils.getTestState;

/** Test for {@link HttpSinkWriter }. */
public class HttpSinkWriterStateSerializerTest {

    private static final ElementConverter<String, HttpSinkRequestEntry> ELEMENT_CONVERTER =
            (s, _context) -> new HttpSinkRequestEntry("POST", s.getBytes(StandardCharsets.UTF_8));

    @Test
    public void testSerializeAndDeserialize() throws IOException {
        BufferedRequestState<HttpSinkRequestEntry> expectedState =
                getTestState(
                        ELEMENT_CONVERTER,
                        httpSinkRequestEntry ->
                                Math.toIntExact(httpSinkRequestEntry.getSizeInBytes()));

        HttpSinkWriterStateSerializer serializer = new HttpSinkWriterStateSerializer();
        BufferedRequestState<HttpSinkRequestEntry> actualState =
                serializer.deserialize(1, serializer.serialize(expectedState));

        assertThatBufferStatesAreEqual(actualState, expectedState);
    }
}
