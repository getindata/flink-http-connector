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

    /**
     * A property prefix for http connector.
     */
    public static final String GID_CONNECTOR_HTTP = "gid.connector.http.";

    /**
     * A property prefix for http connector header properties
     */
    public static final String SINK_HEADER_PREFIX = GID_CONNECTOR_HTTP + "sink.header.";

    /**
     * A property for Content-Type HTTP header.
     */
    public static final String CONTENT_TYPE_HEADER = SINK_HEADER_PREFIX + "Content-Type";

}
