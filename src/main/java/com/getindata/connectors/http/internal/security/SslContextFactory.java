package com.getindata.connectors.http.internal.security;


import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.Key;
import java.security.KeyFactory;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.Base64;
import java.util.Collection;
import java.util.UUID;
import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;

public class SslContextFactory {

    public SSLContext createSslContext(String publicKeyPath, String privateKeyPath)
        throws IOException, CertificateException, NoSuchAlgorithmException,
        InvalidKeySpecException, KeyStoreException, UnrecoverableKeyException,
        KeyManagementException {

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

        Key key = KeyFactory.getInstance("RSA").generatePrivate(new PKCS8EncodedKeySpec(encoded));

        KeyStore clientKeyStore = KeyStore.getInstance("jks");
        final char[] pwdChars = UUID.randomUUID().toString().toCharArray();
        clientKeyStore.load(null, null);
        clientKeyStore.setKeyEntry("test", key, pwdChars, chain.toArray(new Certificate[0]));

        KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance("SunX509");
        keyManagerFactory.init(clientKeyStore, pwdChars);

        return getSslContext(keyManagerFactory.getKeyManagers(), null);
    }

    public static SSLContext getSslContext(KeyManager[] keyManager, TrustManager[] trustManager) {
        try {
            // populate SSLContext with key manager
            SSLContext sslCtx = SSLContext.getInstance("TLSv1.2");
            sslCtx.init(keyManager, trustManager, null);
            return sslCtx;
        } catch (NoSuchAlgorithmException | KeyManagementException e) {
            throw new RuntimeException(e);
        }
    }

    public static TrustManager[] getTrustManagers() {

        try {
            String alg = TrustManagerFactory.getDefaultAlgorithm();
            TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(alg);

            KeyStore ks = KeyStore.getInstance("jks");
            trustManagerFactory.init(ks);

            return trustManagerFactory.getTrustManagers();
        } catch (GeneralSecurityException e) {
            throw new RuntimeException(e);
        }
    }

}
