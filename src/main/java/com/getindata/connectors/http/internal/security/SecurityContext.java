package com.getindata.connectors.http.internal.security;

import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
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
import java.util.UUID;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.util.StringUtils;

@Slf4j
public class SecurityContext {

    public static final String JKS_STORE_TYPE = "jks";

    private final char[] storePasswordCharArr;

    private final KeyStore keystore;

    private SecurityContext(String storePassword) {

        this.storePasswordCharArr = (StringUtils.isNullOrWhitespaceOnly(storePassword)) ?
            UUID.randomUUID().toString().toCharArray() : storePassword.toCharArray();

        try {
            this.keystore = KeyStore.getInstance(JKS_STORE_TYPE);
            this.keystore.load(null, storePasswordCharArr);
            log.info("Created KeyStore for Http Connector security context.");
        } catch (Exception e) {
            throw new RuntimeException(
                "Unable to create KeyStore for Http Connector Security Context.",
                e
            );
        }
    }

    public static SecurityContext contextForLocalStore() {
        return new SecurityContext(null);
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
            log.info("Created security Trust Managers for Http Connector security context.");
            return trustManagerFactory.getTrustManagers();
        } catch (GeneralSecurityException e) {
            throw new RuntimeException(
                "Unable to created Trust Managers for Http Connector security context.",
                e
            );
        }
    }

    public void addCertToTrustStore(String certPath) {

        log.info("Trying to add certificate to Security Context - " + certPath);
        try (FileInputStream certInputStream = new FileInputStream(certPath)) {
            CertificateFactory certificateFactory = CertificateFactory.getInstance("X.509");
            Certificate certificate = certificateFactory.generateCertificate(certInputStream);
            this.keystore.setCertificateEntry(UUID.randomUUID().toString(), certificate);
            log.info("Certificated added to keyStore ass trusted - " + certPath);
        } catch (Exception e) {
            throw new RuntimeException(
                "Unable to add certificate as trusted to Http Connector security context - "
                    + certPath,
                e
            );
        }
    }

    public void addMTlsCerts(String publicKeyPath, String privateKeyPath) {

        try {
            byte[] publicData = Files.readAllBytes(Path.of(publicKeyPath));
            byte[] privateData = Files.readAllBytes(Path.of(privateKeyPath));
            byte[] decodedPrivateData = decodePrivateData(privateKeyPath, privateData);

            CertificateFactory certificateFactory = CertificateFactory.getInstance("X.509");
            Collection<? extends Certificate> chain = certificateFactory.generateCertificates(
                new ByteArrayInputStream(publicData));

            Key key = KeyFactory.getInstance("RSA")
                .generatePrivate(new PKCS8EncodedKeySpec(decodedPrivateData));

            this.keystore.setKeyEntry(
                UUID.randomUUID().toString(),
                key,
                this.storePasswordCharArr,
                chain.toArray(new Certificate[0])
            );
        } catch (Exception e) {
            throw new RuntimeException(
                "Unable to add client private key/public certificate to Http Connector KeyStore. "
                    + String.join(",", privateKeyPath, publicKeyPath),
                e
            );
        }
    }

    private byte[] decodePrivateData(String privateKeyPath, byte[] privateData) {

        // private key must be in PKCS8 format, pem or der.
        // openssl pkcs8 -topk8 -inform PEM -outform PEM -in client.pem
        // -out clientPrivateKey.pem -nocrypt
        if (privateKeyPath.endsWith(".pem") || privateKeyPath.endsWith(".key")) {
            String privateString = new String(privateData, Charset.defaultCharset())
                .replace("-----BEGIN PRIVATE KEY-----", "")
                .replaceAll(System.lineSeparator(), "")
                .replaceAll("\\n", "")
                .replace("-----END PRIVATE KEY-----", "");

            return Base64.getDecoder().decode(privateString);
        } else {
            return privateData;
        }
    }
}
