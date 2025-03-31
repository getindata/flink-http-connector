package com.getindata.connectors.http.internal.retry;

import java.io.IOException;
import java.net.http.HttpClient;
import java.net.http.HttpResponse;
import java.util.function.Supplier;
import java.util.stream.Stream;

import io.github.resilience4j.core.IntervalFunction;
import io.github.resilience4j.retry.RetryConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.getindata.connectors.http.HttpStatusCodeValidationFailedException;
import com.getindata.connectors.http.internal.status.HttpResponseChecker;


@SuppressWarnings("unchecked")
@ExtendWith(MockitoExtension.class)
class HttpClientWithRetryTest {

    @Mock
    private HttpClient httpClient;

    @Mock
    private HttpResponseChecker responseChecker;

    private HttpClientWithRetry client;

    @BeforeEach
    void setup() {
        var retryConfig = RetryConfig.custom()
                .maxAttempts(3)
                .intervalFunction(IntervalFunction.of(1))
                .build();
        client = new HttpClientWithRetry(httpClient, retryConfig, responseChecker);
    }

    @Test
    void shouldRetryOnIOException() throws IOException, InterruptedException, HttpStatusCodeValidationFailedException {
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

        var exception = assertThrows(HttpStatusCodeValidationFailedException.class,
            () -> client.send(mock(Supplier.class), mock(HttpResponse.BodyHandler.class)));

        verify(httpClient, times(3)).send(any(), any());
        assertEquals(response, exception.getResponse());
    }

    @Test
    void shouldFailOnError() throws IOException, InterruptedException {
        var response = mock(HttpResponse.class);
        when(httpClient.send(any(), any())).thenReturn(response);
        when(responseChecker.isSuccessful(response)).thenReturn(false);
        when(responseChecker.isTemporalError(response)).thenReturn(false);

        assertThrows(HttpStatusCodeValidationFailedException.class,
            () -> client.send(mock(Supplier.class), mock(HttpResponse.BodyHandler.class)));

        verify(httpClient, times(1)).send(any(), any());
    }

    @Test
    void shouldHandleUncheckedExceptionFromRetry() throws IOException, InterruptedException {
        when(httpClient.send(any(), any())).thenThrow(RuntimeException.class);

        assertThrows(RuntimeException.class,
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
    void shouldFailOnException(Class<? extends Throwable> exceptionClass) throws IOException, InterruptedException {
        when(httpClient.send(any(), any())).thenThrow(exceptionClass);

        assertThrows(exceptionClass,
            () -> client.send(mock(Supplier.class), mock(HttpResponse.BodyHandler.class)));

        verify(httpClient).send(any(), any());
    }
}
