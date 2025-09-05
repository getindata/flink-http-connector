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

import org.apache.flink.api.connector.sink2.Sink.InitContext;
import org.apache.flink.connector.base.sink.writer.BufferedRequestState;
import org.apache.flink.connector.base.sink.writer.ElementConverter;
import org.apache.flink.connector.http.clients.SinkHttpClient;
import org.apache.flink.connector.http.clients.SinkHttpClientResponse;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.groups.OperatorIOMetricGroup;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Test for {@link HttpSinkWriter }. */
@Slf4j
@ExtendWith(MockitoExtension.class)
class HttpSinkWriterTest {

    private HttpSinkWriter<String> httpSinkWriter;

    @Mock private ElementConverter<String, HttpSinkRequestEntry> elementConverter;

    @Mock private InitContext context;

    @Mock private SinkHttpClient httpClient;

    // To work with Flink 1.15 and Flink 1.16
    @Mock(lenient = true)
    private SinkWriterMetricGroup metricGroup;

    @Mock private OperatorIOMetricGroup operatorIOMetricGroup;

    @Mock private Counter errorCounter;

    @BeforeEach
    public void setUp() {
        when(metricGroup.getNumRecordsSendErrorsCounter()).thenReturn(errorCounter);
        when(metricGroup.getIOMetricGroup()).thenReturn(operatorIOMetricGroup);
        when(context.metricGroup()).thenReturn(metricGroup);

        Collection<BufferedRequestState<HttpSinkRequestEntry>> stateBuffer = new ArrayList<>();

        this.httpSinkWriter =
                new HttpSinkWriter<>(
                        elementConverter,
                        context,
                        10,
                        10,
                        100,
                        10,
                        10,
                        10,
                        "http://localhost/client",
                        httpClient,
                        stateBuffer,
                        new Properties());
    }

    @Test
    public void testErrorMetric() throws InterruptedException {

        CompletableFuture<SinkHttpClientResponse> future = new CompletableFuture<>();
        future.completeExceptionally(new Exception("Test Exception"));

        when(httpClient.putRequests(anyList(), anyString())).thenReturn(future);

        HttpSinkRequestEntry request = new HttpSinkRequestEntry("PUT", "hello".getBytes());
        Consumer<List<HttpSinkRequestEntry>> requestResult =
                httpSinkRequestEntries -> log.info(String.valueOf(httpSinkRequestEntries));

        List<HttpSinkRequestEntry> requestEntries = Collections.singletonList(request);
        this.httpSinkWriter.submitRequestEntries(requestEntries, requestResult);

        // would be good to use Countdown Latch instead sleep...
        Thread.sleep(2000);
        verify(errorCounter).inc(requestEntries.size());
    }
}
