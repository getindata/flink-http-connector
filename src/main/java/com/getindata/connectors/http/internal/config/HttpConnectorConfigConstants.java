package com.getindata.connectors.http.internal.config;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

// TODO EXP-98 add Javadoc
@NoArgsConstructor(access = AccessLevel.NONE)
public final class HttpConnectorConfigConstants {

    public static final String GID_CONNECTOR_HTTP = "gid.connector.http.";

    public static final String SINK_HEADER_PREFIX = GID_CONNECTOR_HTTP + "sink.header.";

    public static final String CONTENT_TYPE_HEADER = SINK_HEADER_PREFIX + "Content-Type";

}
