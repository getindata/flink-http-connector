/* Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.http.retry;

import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.connector.source.lookup.LookupOptions;

import io.github.resilience4j.core.IntervalFunction;
import io.github.resilience4j.retry.RetryConfig;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;

import static io.github.resilience4j.core.IntervalFunction.ofExponentialBackoff;
import static org.apache.flink.connector.http.table.lookup.HttpLookupConnectorOptions.SOURCE_LOOKUP_RETRY_EXPONENTIAL_DELAY_INITIAL_BACKOFF;
import static org.apache.flink.connector.http.table.lookup.HttpLookupConnectorOptions.SOURCE_LOOKUP_RETRY_EXPONENTIAL_DELAY_MAX_BACKOFF;
import static org.apache.flink.connector.http.table.lookup.HttpLookupConnectorOptions.SOURCE_LOOKUP_RETRY_EXPONENTIAL_DELAY_MULTIPLIER;
import static org.apache.flink.connector.http.table.lookup.HttpLookupConnectorOptions.SOURCE_LOOKUP_RETRY_FIXED_DELAY_DELAY;
import static org.apache.flink.connector.http.table.lookup.HttpLookupConnectorOptions.SOURCE_LOOKUP_RETRY_STRATEGY;

/** Configuration for Retry. */
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public class RetryConfigProvider {

    private final ReadableConfig config;

    public static RetryConfig create(ReadableConfig config) {
        return new RetryConfigProvider(config).create();
    }

    private RetryConfig create() {
        return createBuilder().maxAttempts(config.get(LookupOptions.MAX_RETRIES) + 1).build();
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
                .intervalFunction(
                        IntervalFunction.of(config.get(SOURCE_LOOKUP_RETRY_FIXED_DELAY_DELAY)));
    }

    private RetryConfig.Builder<?> configureExponentialDelay() {
        var initialDelay = config.get(SOURCE_LOOKUP_RETRY_EXPONENTIAL_DELAY_INITIAL_BACKOFF);
        var maxDelay = config.get(SOURCE_LOOKUP_RETRY_EXPONENTIAL_DELAY_MAX_BACKOFF);
        var multiplier = config.get(SOURCE_LOOKUP_RETRY_EXPONENTIAL_DELAY_MULTIPLIER);
        return RetryConfig.custom()
                .intervalFunction(ofExponentialBackoff(initialDelay, multiplier, maxDelay));
    }
}
