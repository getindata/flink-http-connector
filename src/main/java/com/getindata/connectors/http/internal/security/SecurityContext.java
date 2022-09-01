package com.getindata.connectors.http.internal.security;


import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.Key;
import java.security.KeyFactory;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.Base64;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;

import org.apache.flink.util.StringUtils;

public class SecurityContext {

    public static final String JKS_STORE_TYPE = "jks";


    private final char[] storePasswordCharArr;

    private final KeyStore keystore;

    public static SecurityContext contextForLocalStore() {
        return new SecurityContext(null);
    }

    public static SecurityContext contextForLocalStore(String storePassword) {
        return new SecurityContext(storePassword);
    }

    private SecurityContext(String storePassword) {

        this.storePasswordCharArr = (StringUtils.isNullOrWhitespaceOnly(storePassword)) ?
            null : storePassword.toCharArray();

        try {
            this.keystore = KeyStore.getInstance(JKS_STORE_TYPE);
            this.keystore.load(null, storePasswordCharArr);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public SSLContext getSslContext(List<TrustManager> trustManagers) {
        return getSslContext(trustManagers.toArray(new TrustManager[0]));
    }

    public SSLContext getSslContext(TrustManager[] trustManagers) {
        try {

            KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance("SunX509");
            keyManagerFactory.init(this.keystore, this.storePasswordCharArr);

            // populate SSLContext with key manager
            SSLContext sslCtx = SSLContext.getInstance("TLSv1.2");
            sslCtx.init(keyManagerFactory.getKeyManagers(), trustManagers, null);
            return sslCtx;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public TrustManager[] getTrustManagers() {

        try {
            String alg = TrustManagerFactory.getDefaultAlgorithm();
            TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(alg);

            trustManagerFactory.init(this.keystore);

            return trustManagerFactory.getTrustManagers();
        } catch (GeneralSecurityException e) {
            throw new RuntimeException(e);
        }
    }

    public void addCertToTrustStore(String certPath) {

        try (FileInputStream certInputStream = new FileInputStream(certPath)) {

            CertificateFactory certificateFactory = CertificateFactory.getInstance("X.509");
            Certificate certificate = certificateFactory.generateCertificate(certInputStream);

            this.keystore.setCertificateEntry(UUID.randomUUID().toString(), certificate);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void addMTlsCerts(String publicKeyPath, String privateKeyPath) {

        try {
            byte[] publicData = Files.readAllBytes(Path.of(publicKeyPath));
            byte[] privateData = Files.readAllBytes(Path.of(privateKeyPath));

            String privateString = new String(privateData, Charset.defaultCharset())
                .replace("-----BEGIN PRIVATE KEY-----", "")
                .replaceAll(System.lineSeparator(), "")
                .replace("-----END PRIVATE KEY-----", "");

            byte[] encoded = Base64.getDecoder().decode(privateString);

            CertificateFactory certificateFactory = CertificateFactory.getInstance("X.509");
            Collection<? extends Certificate> chain = certificateFactory.generateCertificates(
                new ByteArrayInputStream(publicData));

            Key key =
                KeyFactory.getInstance("RSA").generatePrivate(new PKCS8EncodedKeySpec(encoded));

            this.keystore.setKeyEntry(
                UUID.randomUUID().toString(),
                key,
                this.storePasswordCharArr,
                chain.toArray(new Certificate[0])
            );
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void storeKeyStore(String storePath) {
        try (FileOutputStream storeOutputStream = new FileOutputStream(storePath)) {
            this.keystore.store(storeOutputStream, this.storePasswordCharArr);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
