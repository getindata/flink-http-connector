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

package org.apache.flink.connector.http.security;

import lombok.extern.slf4j.Slf4j;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.X509ExtendedTrustManager;
import javax.net.ssl.X509TrustManager;

import java.net.Socket;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;

/** Self signed trust manager. */
@Slf4j
public class SelfSignedTrustManager extends X509ExtendedTrustManager {

    private final X509TrustManager delegate;

    public SelfSignedTrustManager(X509TrustManager delegate) {
        this.delegate = delegate;
    }

    public void checkClientTrusted(X509Certificate[] chain, String s) throws CertificateException {
        this.delegate.checkClientTrusted(chain, s);
    }

    public void checkClientTrusted(X509Certificate[] chain, String s, Socket socket)
            throws CertificateException {
        this.delegate.checkClientTrusted(chain, s);
    }

    public void checkClientTrusted(X509Certificate[] chain, String s, SSLEngine sslEngine)
            throws CertificateException {
        this.delegate.checkClientTrusted(chain, s);
    }

    public void checkServerTrusted(X509Certificate[] chain, String s) throws CertificateException {
        log.info("Allowing self signed server certificates.");
    }

    public void checkServerTrusted(X509Certificate[] chain, String s, Socket socket)
            throws CertificateException {
        log.info("Allowing self signed server certificates.");
    }

    public void checkServerTrusted(X509Certificate[] chain, String s, SSLEngine sslEngine)
            throws CertificateException {
        log.info("Allowing self signed server certificates.");
    }

    public X509Certificate[] getAcceptedIssuers() {
        return this.delegate.getAcceptedIssuers();
    }
}
