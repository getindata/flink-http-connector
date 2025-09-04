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

package org.apache.flink.connector.http.retry;

import org.apache.flink.connector.http.HttpStatusCodeValidationFailedException;
import org.apache.flink.connector.http.status.HttpResponseChecker;

import io.github.resilience4j.core.IntervalFunction;
import io.github.resilience4j.retry.RetryConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.net.http.HttpClient;
import java.net.http.HttpResponse;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Test for {@link HttpClient} with retry. */
@SuppressWarnings("unchecked")
@ExtendWith(MockitoExtension.class)
class HttpClientWithRetryTest {

    @Mock private HttpClient httpClient;

    @Mock private HttpResponseChecker responseChecker;

    private HttpClientWithRetry client;

    @BeforeEach
    void setup() {
        var retryConfig =
                RetryConfig.custom()
                        .maxAttempts(3)
                        .intervalFunction(IntervalFunction.of(1))
                        .build();
        client = new HttpClientWithRetry(httpClient, retryConfig, responseChecker);
    }

    @Test
    void shouldRetryOnIOException()
            throws IOException, InterruptedException, HttpStatusCodeValidationFailedException {
        var response = mock(HttpResponse.class);
        when(httpClient.send(any(), any())).thenThrow(IOException.class).thenReturn(response);
        when(responseChecker.isSuccessful(response)).thenReturn(true);

        var result = client.send(mock(Supplier.class), mock(HttpResponse.BodyHandler.class));

        verify(httpClient, times(2)).send(any(), any());
        assertEquals(response, result);
    }

    @Test
    void shouldRetryOnTemporalException()
            throws IOException, InterruptedException, HttpStatusCodeValidationFailedException {
        var responseA = mock(HttpResponse.class);
        var responseB = mock(HttpResponse.class);
        when(httpClient.send(any(), any())).thenReturn(responseA, responseA, responseB);
        when(responseChecker.isTemporalError(responseA)).thenReturn(true);
        when(responseChecker.isTemporalError(responseB)).thenReturn(false);
        when(responseChecker.isSuccessful(responseB)).thenReturn(true);

        var result = client.send(mock(Supplier.class), mock(HttpResponse.BodyHandler.class));

        verify(httpClient, times(3)).send(any(), any());
        assertEquals(responseB, result);
    }

    @Test
    void shouldFailAfterExceedingMaxRetryAttempts() throws IOException, InterruptedException {
        var response = mock(HttpResponse.class);
        when(httpClient.send(any(), any())).thenReturn(response);
        when(responseChecker.isSuccessful(response)).thenReturn(false);
        when(responseChecker.isTemporalError(response)).thenReturn(true);

        var exception =
                assertThrows(
                        HttpStatusCodeValidationFailedException.class,
                        () ->
                                client.send(
                                        mock(Supplier.class),
                                        mock(HttpResponse.BodyHandler.class)));

        verify(httpClient, times(3)).send(any(), any());
        assertEquals(response, exception.getResponse());
    }

    @Test
    void shouldFailOnError() throws IOException, InterruptedException {
        var response = mock(HttpResponse.class);
        when(httpClient.send(any(), any())).thenReturn(response);
        when(responseChecker.isSuccessful(response)).thenReturn(false);
        when(responseChecker.isTemporalError(response)).thenReturn(false);

        assertThrows(
                HttpStatusCodeValidationFailedException.class,
                () -> client.send(mock(Supplier.class), mock(HttpResponse.BodyHandler.class)));

        verify(httpClient, times(1)).send(any(), any());
    }

    @Test
    void shouldHandleUncheckedExceptionFromRetry() throws IOException, InterruptedException {
        when(httpClient.send(any(), any())).thenThrow(RuntimeException.class);

        assertThrows(
                RuntimeException.class,
                () -> client.send(mock(Supplier.class), mock(HttpResponse.BodyHandler.class)));

        verify(httpClient, times(1)).send(any(), any());
    }

    @Test
    void shouldSendRequestAndProcessSuccessfulResponse()
            throws IOException, InterruptedException, HttpStatusCodeValidationFailedException {
        var response = mock(HttpResponse.class);
        when(httpClient.send(any(), any())).thenReturn(response);
        when(responseChecker.isSuccessful(response)).thenReturn(true);

        var result = client.send(mock(Supplier.class), mock(HttpResponse.BodyHandler.class));

        verify(httpClient).send(any(), any());
        assertEquals(response, result);
    }

    private static Stream<Class<? extends Throwable>> failures() {
        return Stream.of(RuntimeException.class, InterruptedException.class);
    }

    @ParameterizedTest
    @MethodSource("failures")
    void shouldFailOnException(Class<? extends Throwable> exceptionClass)
            throws IOException, InterruptedException {
        when(httpClient.send(any(), any())).thenThrow(exceptionClass);

        assertThrows(
                exceptionClass,
                () -> client.send(mock(Supplier.class), mock(HttpResponse.BodyHandler.class)));

        verify(httpClient).send(any(), any());
    }
}
