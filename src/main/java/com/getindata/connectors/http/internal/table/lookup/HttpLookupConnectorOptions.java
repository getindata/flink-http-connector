package com.getindata.connectors.http.internal.table.lookup;

import java.time.Duration;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

import com.getindata.connectors.http.internal.config.RetryStrategyType;
import static com.getindata.connectors.http.internal.config.HttpConnectorConfigConstants.LOOKUP_SOURCE_HEADER_USE_RAW;
import static com.getindata.connectors.http.internal.config.HttpConnectorConfigConstants.SOURCE_LOOKUP_QUERY_CREATOR_IDENTIFIER;
import static com.getindata.connectors.http.internal.config.HttpConnectorConfigConstants.SOURCE_LOOKUP_REQUEST_CALLBACK_IDENTIFIER;

public class HttpLookupConnectorOptions {

    public static final ConfigOption<String> URL =
        ConfigOptions.key("url")
            .stringType()
            .noDefaultValue()
            .withDescription("The HTTP endpoint URL.");

    public static final ConfigOption<String> URL_ARGS =
        ConfigOptions.key("url-args")
            .stringType()
            .noDefaultValue()
            .withDescription("The arguments that should be used for HTTP GET Request.");

    public static final ConfigOption<Boolean> ASYNC_POLLING =
        ConfigOptions.key("asyncPolling")
            .booleanType()
            .defaultValue(false)
            .withDescription("Whether to use Sync and Async polling mechanism");

    public static final ConfigOption<String> LOOKUP_METHOD =
        ConfigOptions.key("lookup-method")
            .stringType()
            .defaultValue("GET")
            .withDescription("Method used for REST executed by lookup connector.");

    public static final ConfigOption<String> LOOKUP_QUERY_CREATOR_IDENTIFIER =
        ConfigOptions.key(SOURCE_LOOKUP_QUERY_CREATOR_IDENTIFIER)
            .stringType()
            .noDefaultValue();

    public static final ConfigOption<String> LOOKUP_REQUEST_FORMAT =
        ConfigOptions.key("lookup-request.format")
            .stringType()
            .defaultValue("json");

    public static final ConfigOption<Boolean> USE_RAW_AUTH_HEADER =
        ConfigOptions.key(LOOKUP_SOURCE_HEADER_USE_RAW)
            .booleanType()
            .defaultValue(false)
            .withDescription("Whether to use the raw value of Authorization header");

    public static final ConfigOption<String> REQUEST_CALLBACK_IDENTIFIER =
        ConfigOptions.key(SOURCE_LOOKUP_REQUEST_CALLBACK_IDENTIFIER)
            .stringType()
            .defaultValue(Slf4jHttpLookupPostRequestCallbackFactory.IDENTIFIER);

    public static final ConfigOption<RetryStrategyType> LOOKUP_RETRY_STRATEGY_TYPE =
        ConfigOptions.key("lookup.retry-strategy.type")
            .enumType(RetryStrategyType.class)
            .defaultValue(RetryStrategyType.FIXED_DELAY)
            .withDescription("Lookup HTTP request retry strategy.");

    public static final ConfigOption<Integer> LOOKUP_RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS =
        ConfigOptions.key("lookup.retry-strategy.fixed-delay.attempts")
            .intType()
            .defaultValue(3)
            .withDescription("The number of times that the connector retires request"
                + "before the emtpy result is returned.");

    public static final ConfigOption<Duration> LOOKUP_RESTART_STRATEGY_FIXED_DELAY_DELAY =
        ConfigOptions.key("lookup.retry-strategy.fixed-delay.delay")
            .durationType()
            .defaultValue(Duration.ofSeconds(1))
            .withDescription("Delay between two consecutive retry attempts.");

    public static final ConfigOption<Integer> LOOKUP_RESTART_STRATEGY_EXPONENTIAL_DELAY_ATTEMPTS =
        ConfigOptions.key("lookup.retry-strategy.exponential-delay.attempts")
            .intType()
            .defaultValue(3)
            .withDescription("The number of times that the connector retires request"
                + "before the emtpy result is returned.");

    public static final ConfigOption<Duration>
            LOOKUP_RESTART_STRATEGY_EXPONENTIAL_DELAY_INITIAL_DELAY =
        ConfigOptions.key("lookup.retry-strategy.exponential-delay.initial-delay")
            .durationType()
            .defaultValue(Duration.ofSeconds(1))
            .withDescription("Initial delay after the first lookup failure.");

    public static final ConfigOption<Duration> LOOKUP_RESTART_STRATEGY_EXPONENTIAL_DELAY_MAX_DELAY =
        ConfigOptions.key("lookup.retry-strategy.exponential-delay.max-delay")
            .durationType()
            .defaultValue(Duration.ofMinutes(1))
            .withDescription("Maximal delay between two consecutive retry attempts.");
}
