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

    /**
     * A property prefix for http connector header properties
     */
    public static final String SINK_HEADER_PREFIX = GID_CONNECTOR_HTTP + "sink.header.";

    public static final String LOOKUP_SOURCE_HEADER_PREFIX = GID_CONNECTOR_HTTP
        + "source.lookup.header.";

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
    public static final String LOOKUP_SOURCE_HEADER_USE_RAW = GID_CONNECTOR_HTTP
        + "source.lookup.use-raw-authorization-header";

    // --------- Error code handling configuration ---------
    public static final String HTTP_ERROR_SINK_CODE_WHITE_LIST =
        GID_CONNECTOR_HTTP + "sink.error.code.exclude";

    public static final String HTTP_ERROR_SINK_CODES_LIST = GID_CONNECTOR_HTTP + "sink.error.code";

    public static final String HTTP_ERROR_SOURCE_LOOKUP_CODE_WHITE_LIST =
        GID_CONNECTOR_HTTP + "source.lookup.error.code.exclude";

    public static final String HTTP_ERROR_SOURCE_LOOKUP_CODES_LIST =
        GID_CONNECTOR_HTTP + "source.lookup.error.code";
    // -----------------------------------------------------

    public static final String SOURCE_LOOKUP_REQUEST_CALLBACK_IDENTIFIER =
        GID_CONNECTOR_HTTP + "source.lookup.request-callback";

    public static final String SINK_REQUEST_CALLBACK_IDENTIFIER =
        GID_CONNECTOR_HTTP + "sink.request-callback";

    public static final String SOURCE_LOOKUP_QUERY_CREATOR_IDENTIFIER =
        GID_CONNECTOR_HTTP + "source.lookup.query-creator";

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
        GID_CONNECTOR_HTTP + "source.lookup.request.timeout";

    public static final String SINK_HTTP_TIMEOUT_SECONDS =
        GID_CONNECTOR_HTTP + "sink.request.timeout";

    public static final String LOOKUP_HTTP_PULING_THREAD_POOL_SIZE =
        GID_CONNECTOR_HTTP + "source.lookup.request.thread-pool.size";

    public static final String LOOKUP_HTTP_RESPONSE_THREAD_POOL_SIZE =
        GID_CONNECTOR_HTTP + "source.lookup.response.thread-pool.size";

    public static final String SINK_HTTP_WRITER_THREAD_POOL_SIZE =
        GID_CONNECTOR_HTTP + "sink.writer.thread-pool.size";

    // -----------------------------------------------------


    // ------ Sink request submitter settings ------
    public static final String SINK_HTTP_REQUEST_MODE =
        GID_CONNECTOR_HTTP + "sink.writer.request.mode";

    public static final String SINK_HTTP_BATCH_REQUEST_SIZE =
        GID_CONNECTOR_HTTP + "sink.request.batch.size";

    // ---------------------------------------------
}
