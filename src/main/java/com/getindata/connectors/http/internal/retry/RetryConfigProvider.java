package com.getindata.connectors.http.internal.retry;

import io.github.resilience4j.core.IntervalFunction;
import io.github.resilience4j.retry.RetryConfig;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.connector.source.lookup.LookupOptions;
import static io.github.resilience4j.core.IntervalFunction.ofExponentialBackoff;

import static com.getindata.connectors.http.internal.table.lookup.HttpLookupConnectorOptions.SOURCE_LOOKUP_RETRY_EXPONENTIAL_DELAY_INITIAL_BACKOFF;
import static com.getindata.connectors.http.internal.table.lookup.HttpLookupConnectorOptions.SOURCE_LOOKUP_RETRY_EXPONENTIAL_DELAY_MAX_BACKOFF;
import static com.getindata.connectors.http.internal.table.lookup.HttpLookupConnectorOptions.SOURCE_LOOKUP_RETRY_EXPONENTIAL_DELAY_MULTIPLIER;
import static com.getindata.connectors.http.internal.table.lookup.HttpLookupConnectorOptions.SOURCE_LOOKUP_RETRY_FIXED_DELAY_DELAY;
import static com.getindata.connectors.http.internal.table.lookup.HttpLookupConnectorOptions.SOURCE_LOOKUP_RETRY_STRATEGY;

@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public class RetryConfigProvider {

    private final ReadableConfig config;

    public static RetryConfig create(ReadableConfig config) {
        return new RetryConfigProvider(config).create();
    }

    private RetryConfig create() {
        return createBuilder()
                .maxAttempts(config.get(LookupOptions.MAX_RETRIES) + 1)
                .build();
    }

    private RetryConfig.Builder<?> createBuilder() {
        var retryStrategy = getRetryStrategy();
        if (retryStrategy == RetryStrategyType.FIXED_DELAY) {
            return configureFixedDelay();
        } else if (retryStrategy == RetryStrategyType.EXPONENTIAL_DELAY) {
            return configureExponentialDelay();
        }
        throw new IllegalArgumentException("Unsupported retry strategy: " + retryStrategy);
    }

    private RetryStrategyType getRetryStrategy() {
        return RetryStrategyType.fromCode(config.get(SOURCE_LOOKUP_RETRY_STRATEGY));
    }

    private RetryConfig.Builder<?> configureFixedDelay() {
        return RetryConfig.custom()
                .intervalFunction(IntervalFunction.of(config.get(SOURCE_LOOKUP_RETRY_FIXED_DELAY_DELAY)));
    }

    private RetryConfig.Builder<?> configureExponentialDelay() {
        var initialDelay = config.get(SOURCE_LOOKUP_RETRY_EXPONENTIAL_DELAY_INITIAL_BACKOFF);
        var maxDelay = config.get(SOURCE_LOOKUP_RETRY_EXPONENTIAL_DELAY_MAX_BACKOFF);
        var multiplier = config.get(SOURCE_LOOKUP_RETRY_EXPONENTIAL_DELAY_MULTIPLIER);
        return RetryConfig.custom()
                .intervalFunction(ofExponentialBackoff(initialDelay, multiplier, maxDelay));
    }
}
