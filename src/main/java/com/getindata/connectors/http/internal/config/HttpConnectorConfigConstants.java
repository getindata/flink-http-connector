package com.getindata.connectors.http.internal.config;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.experimental.UtilityClass;

/**
 * A dictionary class containing properties or properties prefixes for Http connector.
 */
@UtilityClass
@NoArgsConstructor(access = AccessLevel.NONE)
// TODO Change this name to HttpConnectorConfigProperties
public final class HttpConnectorConfigConstants {

    public static final String PROP_DELIM = ",";

    /**
     * A property prefix for http connector.
     */
    public static final String GID_CONNECTOR_HTTP = "gid.connector.http.";
    private static final String SOURCE_LOOKUP_PREFIX = GID_CONNECTOR_HTTP + "source.lookup.";

    /**
     * A property prefix for http connector header properties
     */
    public static final String SINK_HEADER_PREFIX = GID_CONNECTOR_HTTP + "sink.header.";

    public static final String LOOKUP_SOURCE_HEADER_PREFIX = SOURCE_LOOKUP_PREFIX + "header.";

    public static final String OIDC_AUTH_TOKEN_REQUEST = GID_CONNECTOR_HTTP
            + "security.oidc.token.request";

    public static final String OIDC_AUTH_TOKEN_ENDPOINT_URL = GID_CONNECTOR_HTTP
            + "security.oidc.token.endpoint.url";

    public static final String OIDC_AUTH_TOKEN_EXPIRY_REDUCTION = GID_CONNECTOR_HTTP
            + "security.oidc.token.expiry.reduction";
    /**
     * Whether to use the raw value of the Authorization header. If set, it prevents
     * the special treatment of the header for Basic Authentication, thus preserving the passed
     * raw value. Defaults to false.
     */
    public static final String LOOKUP_SOURCE_HEADER_USE_RAW = SOURCE_LOOKUP_PREFIX + "use-raw-authorization-header";

    public static final String RESULT_TYPE = SOURCE_LOOKUP_PREFIX + "result-type";

    // --------- Error code handling configuration ---------
    public static final String HTTP_ERROR_SINK_CODE_WHITE_LIST = GID_CONNECTOR_HTTP + "sink.error.code.exclude";

    public static final String HTTP_ERROR_SINK_CODES_LIST = GID_CONNECTOR_HTTP + "sink.error.code";
    // -----------------------------------------------------

    public static final String SOURCE_LOOKUP_REQUEST_CALLBACK_IDENTIFIER =
        SOURCE_LOOKUP_PREFIX + "request-callback";

    public static final String SINK_REQUEST_CALLBACK_IDENTIFIER =
        GID_CONNECTOR_HTTP + "sink.request-callback";

    public static final String SOURCE_LOOKUP_QUERY_CREATOR_IDENTIFIER =
        SOURCE_LOOKUP_PREFIX + "query-creator";

    // -------------- HTTPS security settings --------------
    public static final String ALLOW_SELF_SIGNED =
        GID_CONNECTOR_HTTP + "security.cert.server.allowSelfSigned";

    public static final String SERVER_TRUSTED_CERT = GID_CONNECTOR_HTTP + "security.cert.server";

    public static final String CLIENT_CERT = GID_CONNECTOR_HTTP + "security.cert.client";

    public static final String CLIENT_PRIVATE_KEY = GID_CONNECTOR_HTTP + "security.key.client";

    public static final String KEY_STORE_PATH = GID_CONNECTOR_HTTP
        + "security.keystore.path";

    public static final String KEY_STORE_PASSWORD = GID_CONNECTOR_HTTP
        + "security.keystore.password";

    public static final String KEY_STORE_TYPE = GID_CONNECTOR_HTTP
        + "security.keystore.type";

    // -----------------------------------------------------

    // ------ HTTPS timeouts and thread pool settings ------

    public static final String LOOKUP_HTTP_TIMEOUT_SECONDS =
        SOURCE_LOOKUP_PREFIX + "request.timeout";

    public static final String SOURCE_CONNECTION_TIMEOUT =
        SOURCE_LOOKUP_PREFIX + "connection.timeout";

    public static final String SINK_HTTP_TIMEOUT_SECONDS =
        GID_CONNECTOR_HTTP + "sink.request.timeout";

    public static final String LOOKUP_HTTP_PULING_THREAD_POOL_SIZE =
        SOURCE_LOOKUP_PREFIX + "request.thread-pool.size";

    public static final String LOOKUP_HTTP_RESPONSE_THREAD_POOL_SIZE =
        SOURCE_LOOKUP_PREFIX + "response.thread-pool.size";

    public static final String SINK_HTTP_WRITER_THREAD_POOL_SIZE =
        GID_CONNECTOR_HTTP + "sink.writer.thread-pool.size";

    // -----------------------------------------------------


    // ------ Sink request submitter settings ------
    public static final String SINK_HTTP_REQUEST_MODE =
        GID_CONNECTOR_HTTP + "sink.writer.request.mode";

    public static final String SINK_HTTP_BATCH_REQUEST_SIZE =
        GID_CONNECTOR_HTTP + "sink.request.batch.size";

    // ---------------------------------------------
    public static final String SOURCE_RETRY_SUCCESS_CODES = SOURCE_LOOKUP_PREFIX + "success-codes";
    public static final String SOURCE_RETRY_RETRY_CODES = SOURCE_LOOKUP_PREFIX + "retry-codes";
    public static final String SOURCE_IGNORE_RESPONSE_CODES = SOURCE_LOOKUP_PREFIX + "ignored-response-codes";

    public static final String SOURCE_RETRY_STRATEGY_PREFIX = SOURCE_LOOKUP_PREFIX + "retry-strategy.";
    public static final String SOURCE_RETRY_STRATEGY_TYPE = SOURCE_RETRY_STRATEGY_PREFIX + "type";

    private static final String SOURCE_RETRY_FIXED_DELAY_PREFIX = SOURCE_RETRY_STRATEGY_PREFIX + "fixed-delay.";
    public static final String SOURCE_RETRY_FIXED_DELAY_DELAY = SOURCE_RETRY_FIXED_DELAY_PREFIX + "delay";

    private static final String SOURCE_RETRY_EXP_DELAY_PREFIX = SOURCE_RETRY_STRATEGY_PREFIX + "exponential-delay.";
    public static final String SOURCE_RETRY_EXP_DELAY_INITIAL_BACKOFF =
            SOURCE_RETRY_EXP_DELAY_PREFIX + "initial-backoff";
    public static final String SOURCE_RETRY_EXP_DELAY_MAX_BACKOFF =
            SOURCE_RETRY_EXP_DELAY_PREFIX + "max-backoff";
    public static final String SOURCE_RETRY_EXP_DELAY_MULTIPLIER =
            SOURCE_RETRY_EXP_DELAY_PREFIX + "backoff-multiplier";
}
