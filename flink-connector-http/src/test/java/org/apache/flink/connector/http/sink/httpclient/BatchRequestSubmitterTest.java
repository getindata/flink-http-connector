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

package org.apache.flink.connector.http.sink.httpclient;

import org.apache.flink.connector.http.config.HttpConnectorConfigConstants;
import org.apache.flink.connector.http.sink.HttpSinkRequestEntry;

import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.net.http.HttpClient;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Test for {@link BatchRequestSubmitter}. */
@ExtendWith(MockitoExtension.class)
class BatchRequestSubmitterTest {

    @Mock private HttpClient mockHttpClient;

    @ParameterizedTest
    @CsvSource(value = {"50, 1", "5, 1", "3, 2", "2, 3", "1, 5"})
    public void submitBatches(int batchSize, int expectedNumberOfBatchRequests) {

        Properties properties = new Properties();
        properties.setProperty(
                HttpConnectorConfigConstants.SINK_HTTP_BATCH_REQUEST_SIZE,
                String.valueOf(batchSize));

        when(mockHttpClient.sendAsync(any(), any())).thenReturn(new CompletableFuture<>());

        BatchRequestSubmitter submitter =
                new BatchRequestSubmitter(properties, new String[0], mockHttpClient);

        submitter.submit(
                "http://hello.pl",
                IntStream.range(0, 5)
                        .mapToObj(val -> new HttpSinkRequestEntry("PUT", new byte[0]))
                        .collect(Collectors.toList()));

        verify(mockHttpClient, times(expectedNumberOfBatchRequests)).sendAsync(any(), any());
    }

    private static Stream<Arguments> httpRequestMethods() {
        return Stream.of(
                Arguments.of(List.of("PUT", "PUT", "PUT", "PUT", "POST"), 2),
                Arguments.of(List.of("PUT", "PUT", "PUT", "POST", "PUT"), 3),
                Arguments.of(List.of("POST", "PUT", "POST", "POST", "PUT"), 4));
    }

    @ParameterizedTest
    @MethodSource("httpRequestMethods")
    public void shouldSplitBatchPerHttpMethod(
            List<String> httpMethods, int expectedNumberOfBatchRequests) {

        Properties properties = new Properties();
        properties.setProperty(
                HttpConnectorConfigConstants.SINK_HTTP_BATCH_REQUEST_SIZE, String.valueOf(50));

        when(mockHttpClient.sendAsync(any(), any())).thenReturn(new CompletableFuture<>());

        BatchRequestSubmitter submitter =
                new BatchRequestSubmitter(properties, new String[0], mockHttpClient);

        submitter.submit(
                "http://hello.pl",
                httpMethods.stream()
                        .map(method -> new HttpSinkRequestEntry(method, new byte[0]))
                        .collect(Collectors.toList()));

        verify(mockHttpClient, times(expectedNumberOfBatchRequests)).sendAsync(any(), any());
    }
}
