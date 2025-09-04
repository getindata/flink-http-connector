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

package org.apache.flink.connector.http.config;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.experimental.UtilityClass;

/** A dictionary class containing properties or properties prefixes for Http connector. */
@UtilityClass
@NoArgsConstructor(access = AccessLevel.NONE)
// TODO Change this name to HttpConnectorConfigProperties
public final class HttpConnectorConfigConstants {

    public static final String PROP_DELIM = ",";

    /** A property prefix for http connector. */
    public static final String FLINK_CONNECTOR_HTTP = "http.";

    private static final String SOURCE_LOOKUP_PREFIX = FLINK_CONNECTOR_HTTP + "source.lookup.";

    /** A property prefix for http connector header properties. */
    public static final String SINK_HEADER_PREFIX = FLINK_CONNECTOR_HTTP + "sink.header.";

    public static final String LOOKUP_SOURCE_HEADER_PREFIX = SOURCE_LOOKUP_PREFIX + "header.";
    public static final String OIDC_AUTH_TOKEN_REQUEST =
            FLINK_CONNECTOR_HTTP + "security.oidc.token.request";

    public static final String OIDC_AUTH_TOKEN_ENDPOINT_URL =
            FLINK_CONNECTOR_HTTP + "security.oidc.token.endpoint.url";

    public static final String OIDC_AUTH_TOKEN_EXPIRY_REDUCTION =
            FLINK_CONNECTOR_HTTP + "security.oidc.token.expiry.reduction";
    /**
     * Whether to use the raw value of the Authorization header. If set, it prevents the special
     * treatment of the header for Basic Authentication, thus preserving the passed raw value.
     * Defaults to false.
     */
    public static final String LOOKUP_SOURCE_HEADER_USE_RAW =
            SOURCE_LOOKUP_PREFIX + "use-raw-authorization-header";

    public static final String RESULT_TYPE = SOURCE_LOOKUP_PREFIX + "result-type";

    // --------- Error code handling configuration ---------

    // TODO copied from
    // https://github.com/getindata/flink-http-connector/blob/e00d57607f7d1a0d72c6ca48abe[…]nnectors/http/internal/config/HttpConnectorConfigConstants.java
    // Changing label name to INCLUDE, but the value is exclude. Needs investigating.
    public static final String HTTP_ERROR_SINK_CODE_INCLUDE_LIST =
            FLINK_CONNECTOR_HTTP + "sink.error.code.exclude";

    public static final String HTTP_ERROR_SINK_CODES_LIST =
            FLINK_CONNECTOR_HTTP + "sink.error.code";
    // -----------------------------------------------------

    public static final String SOURCE_LOOKUP_REQUEST_CALLBACK_IDENTIFIER =
            SOURCE_LOOKUP_PREFIX + "request-callback";

    public static final String SINK_REQUEST_CALLBACK_IDENTIFIER =
            FLINK_CONNECTOR_HTTP + "sink.request-callback";

    public static final String SOURCE_LOOKUP_QUERY_CREATOR_IDENTIFIER =
            SOURCE_LOOKUP_PREFIX + "query-creator";

    // -------------- HTTPS security settings --------------
    public static final String ALLOW_SELF_SIGNED =
            FLINK_CONNECTOR_HTTP + "security.cert.server.allowSelfSigned";

    public static final String SERVER_TRUSTED_CERT = FLINK_CONNECTOR_HTTP + "security.cert.server";

    public static final String CLIENT_CERT = FLINK_CONNECTOR_HTTP + "security.cert.client";

    public static final String CLIENT_PRIVATE_KEY = FLINK_CONNECTOR_HTTP + "security.key.client";

    public static final String KEY_STORE_PATH = FLINK_CONNECTOR_HTTP + "security.keystore.path";

    public static final String KEY_STORE_PASSWORD =
            FLINK_CONNECTOR_HTTP + "security.keystore.password";

    public static final String KEY_STORE_TYPE = FLINK_CONNECTOR_HTTP + "security.keystore.type";

    // -----------------------------------------------------

    // ------ HTTPS timeouts and thread pool settings ------

    public static final String LOOKUP_HTTP_TIMEOUT_SECONDS =
            SOURCE_LOOKUP_PREFIX + "request.timeout";

    public static final String SOURCE_CONNECTION_TIMEOUT =
            SOURCE_LOOKUP_PREFIX + "connection.timeout";

    public static final String SOURCE_PROXY_HOST = SOURCE_LOOKUP_PREFIX + "proxy.host";

    public static final String SOURCE_PROXY_PORT = SOURCE_LOOKUP_PREFIX + "proxy.port";

    public static final String SOURCE_PROXY_USERNAME = SOURCE_LOOKUP_PREFIX + "proxy.username";

    public static final String SOURCE_PROXY_PASSWORD = SOURCE_LOOKUP_PREFIX + "proxy.password";

    public static final String SINK_HTTP_TIMEOUT_SECONDS =
            FLINK_CONNECTOR_HTTP + "sink.request.timeout";

    public static final String LOOKUP_HTTP_PULING_THREAD_POOL_SIZE =
            SOURCE_LOOKUP_PREFIX + "request.thread-pool.size";

    public static final String LOOKUP_HTTP_RESPONSE_THREAD_POOL_SIZE =
            SOURCE_LOOKUP_PREFIX + "response.thread-pool.size";

    public static final String SINK_HTTP_WRITER_THREAD_POOL_SIZE =
            FLINK_CONNECTOR_HTTP + "sink.writer.thread-pool.size";

    // -----------------------------------------------------

    // ------ Sink request submitter settings ------
    public static final String SINK_HTTP_REQUEST_MODE =
            FLINK_CONNECTOR_HTTP + "sink.writer.request.mode";

    public static final String SINK_HTTP_BATCH_REQUEST_SIZE =
            FLINK_CONNECTOR_HTTP + "sink.request.batch.size";

    // ---------------------------------------------
    public static final String SOURCE_RETRY_SUCCESS_CODES = SOURCE_LOOKUP_PREFIX + "success-codes";
    public static final String SOURCE_RETRY_RETRY_CODES = SOURCE_LOOKUP_PREFIX + "retry-codes";
    public static final String SOURCE_IGNORE_RESPONSE_CODES =
            SOURCE_LOOKUP_PREFIX + "ignored-response-codes";

    public static final String SOURCE_RETRY_STRATEGY_PREFIX =
            SOURCE_LOOKUP_PREFIX + "retry-strategy.";
    public static final String SOURCE_RETRY_STRATEGY_TYPE = SOURCE_RETRY_STRATEGY_PREFIX + "type";

    private static final String SOURCE_RETRY_FIXED_DELAY_PREFIX =
            SOURCE_RETRY_STRATEGY_PREFIX + "fixed-delay.";
    public static final String SOURCE_RETRY_FIXED_DELAY_DELAY =
            SOURCE_RETRY_FIXED_DELAY_PREFIX + "delay";

    private static final String SOURCE_RETRY_EXP_DELAY_PREFIX =
            SOURCE_RETRY_STRATEGY_PREFIX + "exponential-delay.";
    public static final String SOURCE_RETRY_EXP_DELAY_INITIAL_BACKOFF =
            SOURCE_RETRY_EXP_DELAY_PREFIX + "initial-backoff";
    public static final String SOURCE_RETRY_EXP_DELAY_MAX_BACKOFF =
            SOURCE_RETRY_EXP_DELAY_PREFIX + "max-backoff";
    public static final String SOURCE_RETRY_EXP_DELAY_MULTIPLIER =
            SOURCE_RETRY_EXP_DELAY_PREFIX + "backoff-multiplier";
}
