package com.getindata.connectors.http.internal.retry;

import java.util.stream.IntStream;

import org.apache.flink.configuration.Configuration;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;

class RetryConfigProviderTest {

    @Test
    void verifyFixedDelayRetryConfig() {
        var config = new Configuration();
        config.setString("gid.connector.http.source.lookup.retry-strategy.type", "fixed-delay");
        config.setString("gid.connector.http.source.lookup.retry-strategy.fixed-delay.delay", "10s");
        config.setInteger("lookup.max-retries", 12);

        var retryConfig = RetryConfigProvider.create(config);

        assertEquals(13, retryConfig.getMaxAttempts());
        IntStream.range(1, 12).forEach(attempt ->
                assertEquals(10000, retryConfig.getIntervalFunction().apply(attempt))
        );
    }

    @Test
    void verifyExponentialDelayConfig() {
        var config = new Configuration();
        config.setString("gid.connector.http.source.lookup.retry-strategy.type", "exponential-delay");
        config.setString("gid.connector.http.source.lookup.retry-strategy.exponential-delay.initial-backoff", "15ms");
        config.setString("gid.connector.http.source.lookup.retry-strategy.exponential-delay.max-backoff", "120ms");
        config.setInteger("gid.connector.http.source.lookup.retry-strategy.exponential-delay.backoff-multiplier", 2);
        config.setInteger("lookup.max-retries", 6);

        var retryConfig = RetryConfigProvider.create(config);
        var intervalFunction = retryConfig.getIntervalFunction();

        assertEquals(7, retryConfig.getMaxAttempts());
        assertEquals(15, intervalFunction.apply(1));
        assertEquals(30, intervalFunction.apply(2));
        assertEquals(60, intervalFunction.apply(3));
        assertEquals(120, intervalFunction.apply(4));
        assertEquals(120, intervalFunction.apply(5));
        assertEquals(120, intervalFunction.apply(6));
    }

    @Test
    void failWhenStrategyIsUnsupported() {
        var config = new Configuration();
        config.setString("gid.connector.http.source.lookup.retry-strategy.type", "dummy");

        try (var mockedStatic = mockStatic(RetryStrategyType.class)) {
            var dummyStrategy = mock(RetryStrategyType.class);
            mockedStatic.when(() -> RetryStrategyType.fromCode("dummy")).thenReturn(dummyStrategy);

            assertThrows(IllegalArgumentException.class,
                () -> RetryConfigProvider.create(config));
        }
    }
}
