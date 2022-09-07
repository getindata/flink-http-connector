package com.getindata.connectors.http.internal.security;

import java.net.Socket;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.X509ExtendedTrustManager;
import javax.net.ssl.X509TrustManager;

import lombok.extern.slf4j.Slf4j;

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
