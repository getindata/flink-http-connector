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

package org.apache.flink.connector.http.utils;

import lombok.Getter;

import java.net.Authenticator;
import java.net.PasswordAuthentication;
import java.util.Optional;

/** proxy config. */
@Getter
public class ProxyConfig {

    private final String host;

    private final int port;

    private final Optional<Authenticator> authenticator;

    public ProxyConfig(
            String host, int port, Optional<String> proxyUsername, Optional<String> proxyPassword) {
        this.host = host;
        this.port = port;

        if (proxyUsername.isPresent() && proxyPassword.isPresent()) {
            this.authenticator =
                    Optional.of(
                            new Authenticator() {
                                @Override
                                protected PasswordAuthentication getPasswordAuthentication() {
                                    if (getRequestorType().equals(RequestorType.PROXY)
                                            && getRequestingHost().equalsIgnoreCase(host)) {
                                        return new PasswordAuthentication(
                                                proxyUsername.get(),
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
