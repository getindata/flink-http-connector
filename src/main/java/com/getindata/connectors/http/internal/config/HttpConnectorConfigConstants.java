package com.getindata.connectors.http.internal.config;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.experimental.UtilityClass;

/**
 * A dictionary class containing properties or properties prefixes for Http connector.
 */
@UtilityClass
@NoArgsConstructor(access = AccessLevel.NONE)
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

    // --------- Error code handling configuration ---------
    public static final String HTTP_ERROR_SINK_CODE_WHITE_LIST =
        GID_CONNECTOR_HTTP + "sink.error.code.exclude";

    public static final String HTTP_ERROR_SINK_CODES_LIST = GID_CONNECTOR_HTTP + "sink.error.code";

    public static final String HTTP_ERROR_SOURCE_LOOKUP_CODE_WHITE_LIST =
        GID_CONNECTOR_HTTP + "source.lookup.error.code.exclude";

    public static final String HTTP_ERROR_SOURCE_LOOKUP_CODES_LIST =
        GID_CONNECTOR_HTTP + "source.lookup.error.code";
    // -----------------------------------------------------

    public static final String SINK_REQUEST_CALLBACK_IDENTIFIER =
        GID_CONNECTOR_HTTP + "sink.request-callback";

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
}
