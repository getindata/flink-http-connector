package com.getindata.connectors.http.internal.utils;

import lombok.Getter;

@Getter
public class ProxyConfig {

    private final String host;

    private final int port;

    public ProxyConfig(String proxyString) {
        String host = proxyString.substring(0, proxyString.lastIndexOf(':'));
        int port = Integer.parseInt(proxyString.substring(proxyString.lastIndexOf(':') + 1));
        this.host = host;
        this.port = port;
    }

}
