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

    public static final String HTTP_CLIENT_BEAN_NAME = GID_CONNECTOR_HTTP + "client.bean.name";

    public static final String BEAN_CONFIGURATION_PACKAGE = GID_CONNECTOR_HTTP
        + "configuration.package";

    public static final String FACTORY_NAME = GID_CONNECTOR_HTTP + "factory";

}
