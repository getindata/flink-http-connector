package com.getindata.connectors.http.internal.utils;

import java.net.Authenticator;
import java.net.PasswordAuthentication;
import java.util.Optional;

import lombok.Getter;

@Getter
public class ProxyConfig {

    private final String host;

    private final int port;

    private final Optional<Authenticator> authenticator;

    public ProxyConfig(String host, int port, Optional<String> proxyUsername, Optional<String> proxyPassword) {
        this.host = host;
        this.port = port;

        if(proxyUsername.isPresent() && proxyPassword.isPresent()){
            this.authenticator = Optional.of(new Authenticator() {
                @Override
                protected PasswordAuthentication getPasswordAuthentication() {
                    if (getRequestorType().equals(RequestorType.PROXY) && getRequestingHost().equalsIgnoreCase(host)) {
                        return new PasswordAuthentication(proxyUsername.get(),
                                proxyPassword.get().toCharArray());
                    } else {
                        return null;
                    }
                }
            });
        } else {
            this.authenticator = Optional.empty();
        }

    }

}
