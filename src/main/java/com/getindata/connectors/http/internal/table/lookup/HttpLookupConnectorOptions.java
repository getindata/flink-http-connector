package com.getindata.connectors.http.internal.table.lookup;

import java.time.Duration;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

import com.getindata.connectors.http.internal.retry.RetryStrategyType;
import static com.getindata.connectors.http.internal.config.HttpConnectorConfigConstants.*;

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

    public static final ConfigOption<String> SOURCE_LOOKUP_OIDC_AUTH_TOKEN_ENDPOINT_URL =
            ConfigOptions.key(OIDC_AUTH_TOKEN_ENDPOINT_URL)
                    .stringType()
                    .noDefaultValue()
                    .withDescription("OIDC Token endpoint url.");

    public static final ConfigOption<String> SOURCE_LOOKUP_OIDC_AUTH_TOKEN_REQUEST =
            ConfigOptions.key(OIDC_AUTH_TOKEN_REQUEST)
                    .stringType()
                    .noDefaultValue()
                    .withDescription("OIDC token request.");

    public static final ConfigOption<Duration> SOURCE_LOOKUP_OIDC_AUTH_TOKEN_EXPIRY_REDUCTION =
            ConfigOptions.key(OIDC_AUTH_TOKEN_EXPIRY_REDUCTION)
                    .durationType()
                    .defaultValue(Duration.ofSeconds(1))
                    .withDescription("OIDC authorization access token expiry" +
                            " reduction as a Duration." +
                            " A new access token is obtained if the token" +
                            " is older than it's expiry time minus this value.");

    public static final ConfigOption<Duration> SOURCE_LOOKUP_CONNECTION_TIMEOUT =
            ConfigOptions.key(SOURCE_CONNECTION_TIMEOUT)
                    .durationType()
                    .noDefaultValue()
                    .withDescription("Http client connection timeout.");

    public static final ConfigOption<String> SOURCE_LOOKUP_RETRY_STRATEGY =
            ConfigOptions.key(SOURCE_RETRY_STRATEGY_TYPE)
                    .stringType()
                    .defaultValue(RetryStrategyType.FIXED_DELAY.getCode())
                    .withDescription("Auto retry strategy type: fixed-delay (default) or exponential-delay.");

    public static final ConfigOption<String> SOURCE_LOOKUP_HTTP_SUCCESS_CODES =
            ConfigOptions.key(SOURCE_RETRY_SUCCESS_CODES)
                    .stringType()
                    .defaultValue("2XX")
                    .withDescription("Comma separated http codes considered as success response. " +
                            "Use [1-5]XX for groups and '!' character for excluding.");

    public static final ConfigOption<String> SOURCE_LOOKUP_HTTP_RETRY_CODES =
            ConfigOptions.key(SOURCE_RETRY_RETRY_CODES)
                    .stringType()
                    .defaultValue("500,503,504")
                    .withDescription("Comma separated http codes considered as transient errors. " +
                            "Use [1-5]XX for groups and '!' character for excluding.");

    public static final ConfigOption<Duration> SOURCE_LOOKUP_RETRY_FIXED_DELAY_DELAY =
            ConfigOptions.key(SOURCE_RETRY_FIXED_DELAY_DELAY)
                    .durationType()
                    .defaultValue(Duration.ofSeconds(1))
                    .withDescription("Fixed-delay interval between retries.");

    public static final ConfigOption<Duration> SOURCE_LOOKUP_RETRY_EXPONENTIAL_DELAY_INITIAL_BACKOFF =
            ConfigOptions.key(SOURCE_RETRY_EXP_DELAY_INITIAL_BACKOFF)
                    .durationType()
                    .defaultValue(Duration.ofSeconds(1))
                    .withDescription("Exponential-delay initial delay.");

    public static final ConfigOption<Duration> SOURCE_LOOKUP_RETRY_EXPONENTIAL_DELAY_MAX_BACKOFF =
            ConfigOptions.key(SOURCE_RETRY_EXP_DELAY_MAX_BACKOFF)
                    .durationType()
                    .defaultValue(Duration.ofMinutes(1))
                    .withDescription("Exponential-delay maximum delay.");

    public static final ConfigOption<Double> SOURCE_LOOKUP_RETRY_EXPONENTIAL_DELAY_MULTIPLIER =
            ConfigOptions.key(SOURCE_RETRY_EXP_DELAY_MULTIPLIER)
                    .doubleType()
                    .defaultValue(1.5)
                    .withDescription("Exponential-delay multiplier.");

    public static final ConfigOption<String> SOURCE_LOOKUP_HTTP_IGNORED_RESPONSE_CODES =
            ConfigOptions.key(SOURCE_IGNORE_RESPONSE_CODES)
                    .stringType()
                    .defaultValue("")
                    .withDescription("Comma separated http codes. Content for these responses will be ignored. " +
                        "Use [1-5]XX for groups and '!' character for excluding. " +
                            "Ignored responses togater with `" + SOURCE_RETRY_SUCCESS_CODES
                            + "` are considered as successful.");
}
