/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
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

package org.apache.flink.connector.http.table.lookup;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.connector.http.retry.RetryStrategyType;

import java.time.Duration;

import static org.apache.flink.connector.http.config.HttpConnectorConfigConstants.LOOKUP_SOURCE_HEADER_USE_RAW;
import static org.apache.flink.connector.http.config.HttpConnectorConfigConstants.OIDC_AUTH_TOKEN_ENDPOINT_URL;
import static org.apache.flink.connector.http.config.HttpConnectorConfigConstants.OIDC_AUTH_TOKEN_EXPIRY_REDUCTION;
import static org.apache.flink.connector.http.config.HttpConnectorConfigConstants.OIDC_AUTH_TOKEN_REQUEST;
import static org.apache.flink.connector.http.config.HttpConnectorConfigConstants.SOURCE_CONNECTION_TIMEOUT;
import static org.apache.flink.connector.http.config.HttpConnectorConfigConstants.SOURCE_IGNORE_RESPONSE_CODES;
import static org.apache.flink.connector.http.config.HttpConnectorConfigConstants.SOURCE_LOOKUP_QUERY_CREATOR_IDENTIFIER;
import static org.apache.flink.connector.http.config.HttpConnectorConfigConstants.SOURCE_LOOKUP_REQUEST_CALLBACK_IDENTIFIER;
import static org.apache.flink.connector.http.config.HttpConnectorConfigConstants.SOURCE_PROXY_HOST;
import static org.apache.flink.connector.http.config.HttpConnectorConfigConstants.SOURCE_PROXY_PASSWORD;
import static org.apache.flink.connector.http.config.HttpConnectorConfigConstants.SOURCE_PROXY_PORT;
import static org.apache.flink.connector.http.config.HttpConnectorConfigConstants.SOURCE_PROXY_USERNAME;
import static org.apache.flink.connector.http.config.HttpConnectorConfigConstants.SOURCE_RETRY_EXP_DELAY_INITIAL_BACKOFF;
import static org.apache.flink.connector.http.config.HttpConnectorConfigConstants.SOURCE_RETRY_EXP_DELAY_MAX_BACKOFF;
import static org.apache.flink.connector.http.config.HttpConnectorConfigConstants.SOURCE_RETRY_EXP_DELAY_MULTIPLIER;
import static org.apache.flink.connector.http.config.HttpConnectorConfigConstants.SOURCE_RETRY_FIXED_DELAY_DELAY;
import static org.apache.flink.connector.http.config.HttpConnectorConfigConstants.SOURCE_RETRY_RETRY_CODES;
import static org.apache.flink.connector.http.config.HttpConnectorConfigConstants.SOURCE_RETRY_STRATEGY_TYPE;
import static org.apache.flink.connector.http.config.HttpConnectorConfigConstants.SOURCE_RETRY_SUCCESS_CODES;

/** http lookup connector options. */
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
            ConfigOptions.key(SOURCE_LOOKUP_QUERY_CREATOR_IDENTIFIER).stringType().noDefaultValue();

    public static final ConfigOption<String> LOOKUP_REQUEST_FORMAT =
            ConfigOptions.key("lookup-request.format").stringType().defaultValue("json");

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
                    .withDescription(
                            "OIDC authorization access token expiry"
                                    + " reduction as a Duration."
                                    + " A new access token is obtained if the token"
                                    + " is older than it's expiry time minus this value.");

    public static final ConfigOption<Duration> SOURCE_LOOKUP_CONNECTION_TIMEOUT =
            ConfigOptions.key(SOURCE_CONNECTION_TIMEOUT)
                    .durationType()
                    .noDefaultValue()
                    .withDescription("Http client connection timeout.");

    public static final ConfigOption<String> SOURCE_LOOKUP_PROXY_HOST =
            ConfigOptions.key(SOURCE_PROXY_HOST)
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Http client proxy host.");

    public static final ConfigOption<Integer> SOURCE_LOOKUP_PROXY_PORT =
            ConfigOptions.key(SOURCE_PROXY_PORT)
                    .intType()
                    .noDefaultValue()
                    .withDescription("Http client proxy port.");

    public static final ConfigOption<String> SOURCE_LOOKUP_PROXY_USERNAME =
            ConfigOptions.key(SOURCE_PROXY_USERNAME)
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Http client proxy username for authentication.");

    public static final ConfigOption<String> SOURCE_LOOKUP_PROXY_PASSWORD =
            ConfigOptions.key(SOURCE_PROXY_PASSWORD)
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Http client proxy password for authentication.");

    public static final ConfigOption<String> SOURCE_LOOKUP_RETRY_STRATEGY =
            ConfigOptions.key(SOURCE_RETRY_STRATEGY_TYPE)
                    .stringType()
                    .defaultValue(RetryStrategyType.FIXED_DELAY.getCode())
                    .withDescription(
                            "Auto retry strategy type: fixed-delay (default) or exponential-delay.");

    public static final ConfigOption<String> SOURCE_LOOKUP_HTTP_SUCCESS_CODES =
            ConfigOptions.key(SOURCE_RETRY_SUCCESS_CODES)
                    .stringType()
                    .defaultValue("2XX")
                    .withDescription(
                            "Comma separated http codes considered as success response. "
                                    + "Use [1-5]XX for groups and '!' character for excluding.");

    public static final ConfigOption<String> SOURCE_LOOKUP_HTTP_RETRY_CODES =
            ConfigOptions.key(SOURCE_RETRY_RETRY_CODES)
                    .stringType()
                    .defaultValue("500,503,504")
                    .withDescription(
                            "Comma separated http codes considered as transient errors. "
                                    + "Use [1-5]XX for groups and '!' character for excluding.");

    public static final ConfigOption<Duration> SOURCE_LOOKUP_RETRY_FIXED_DELAY_DELAY =
            ConfigOptions.key(SOURCE_RETRY_FIXED_DELAY_DELAY)
                    .durationType()
                    .defaultValue(Duration.ofSeconds(1))
                    .withDescription("Fixed-delay interval between retries.");

    public static final ConfigOption<Duration>
            SOURCE_LOOKUP_RETRY_EXPONENTIAL_DELAY_INITIAL_BACKOFF =
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
                    .withDescription(
                            "Comma separated http codes. Content for these responses will be ignored. "
                                    + "Use [1-5]XX for groups and '!' character for excluding. "
                                    + "Ignored responses togater with `"
                                    + SOURCE_RETRY_SUCCESS_CODES
                                    + "` are considered as successful.");
}
