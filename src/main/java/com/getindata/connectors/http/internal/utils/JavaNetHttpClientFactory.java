package com.getindata.connectors.http.internal.utils;

import java.security.KeyStore;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import javax.net.ssl.*;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import okhttp3.Dispatcher;
import okhttp3.OkHttpClient;
import org.apache.flink.util.StringUtils;

import com.getindata.connectors.http.internal.config.HttpConnectorConfigConstants;
import com.getindata.connectors.http.internal.security.SecurityContext;
import com.getindata.connectors.http.internal.security.SelfSignedTrustManager;

@Slf4j
@NoArgsConstructor(access = AccessLevel.NONE)
public class JavaNetHttpClientFactory {

    /**
     * Creates Java's {@link HttpClient} instance that will be using default, JVM shared {@link
     * java.util.concurrent.ForkJoinPool} for async calls.
     *
     * @param properties properties used to build {@link SSLContext}
     * @return new {@link HttpClient} instance.
     */
    public static OkHttpClient createClient(Properties properties) {

//        SSLContext sslContext = getSslContext(properties);
        return new OkHttpClient.Builder()
                .followRedirects(true)
//                .sslSocketFactory(sslContext.getSocketFactory(), (X509TrustManager) getTrustedManagers()[0])
                .sslSocketFactory(getSSLSocketFactory(),  getX509TrustManager())
                .hostnameVerifier(getHostnameVerifier())
                .build();
    }

    /**
     * Creates Java's {@link HttpClient} instance that will be using provided Executor for all async
     * calls.
     *
     * @param properties properties used to build {@link SSLContext}
     * @param executor   {@link Executor} for async calls.
     * @return new {@link HttpClient} instance.
     */
    public static OkHttpClient createClient(Properties properties, ExecutorService executor) {

//        SSLContext sslContext = getSslContext(properties);

        return new OkHttpClient.Builder().followRedirects(true)
//                .sslSocketFactory(sslContext.getSocketFactory(), (X509TrustManager) getTrustedManagers()[0])
                .sslSocketFactory(getSSLSocketFactory(),  getX509TrustManager())
                .hostnameVerifier(getHostnameVerifier())
                .dispatcher(createDispatcher(executor))
                .build();
    }

    public static Dispatcher createDispatcher(ExecutorService executorService) {
        Dispatcher dispatcher = new Dispatcher(executorService);
        dispatcher.setMaxRequests(64);
        dispatcher.setMaxRequestsPerHost(16);
        return dispatcher;
    }


    /**
     * description 忽略https证书验证
     */
    private static SSLSocketFactory getSSLSocketFactory() {
        try {
            SSLContext sslContext = SSLContext.getInstance("SSL");
            sslContext.init(null, getTrustedManagers(), new SecureRandom());
            return sslContext.getSocketFactory();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @SneakyThrows
    private static X509TrustManager getX509TrustManager() {
        X509TrustManager trustManager = null;
        try {
            TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            trustManagerFactory.init((KeyStore) null);
            TrustManager[] trustManagers = trustManagerFactory.getTrustManagers();
            if (trustManagers.length != 1 || !(trustManagers[0] instanceof X509TrustManager)) {
                throw new IllegalStateException("Unexpected default trust managers:" + Arrays.toString(trustManagers));
            }
            trustManager = (X509TrustManager) trustManagers[0];
        } catch (Exception e) {
            log.error("create X509TrustManager error", e);
            throw e;
        }

        return trustManager;
    }

    /**
     * Creates an {@link SSLContext} based on provided properties.
     * <ul>
     *     <li>{@link HttpConnectorConfigConstants#ALLOW_SELF_SIGNED}</li>
     *     <li>{@link HttpConnectorConfigConstants#SERVER_TRUSTED_CERT}</li>
     *     <li>{@link HttpConnectorConfigConstants#PROP_DELIM}</li>
     *     <li>{@link HttpConnectorConfigConstants#CLIENT_CERT}</li>
     *     <li>{@link HttpConnectorConfigConstants#CLIENT_PRIVATE_KEY}</li>
     * </ul>
     *
     * @param properties properties used to build {@link SSLContext}
     * @return new {@link SSLContext} instance.
     */
    private static SSLContext getSslContext(Properties properties) {
        SecurityContext securityContext = createSecurityContext(properties);

        boolean selfSignedCert = Boolean.parseBoolean(
            properties.getProperty(HttpConnectorConfigConstants.ALLOW_SELF_SIGNED, "false"));

        String[] serverTrustedCerts = properties
            .getProperty(HttpConnectorConfigConstants.SERVER_TRUSTED_CERT, "")
            .split(HttpConnectorConfigConstants.PROP_DELIM);

        String clientCert = properties
            .getProperty(HttpConnectorConfigConstants.CLIENT_CERT, "");

        String clientPrivateKey = properties
            .getProperty(HttpConnectorConfigConstants.CLIENT_PRIVATE_KEY, "");

        for (String cert : serverTrustedCerts) {
            if (!StringUtils.isNullOrWhitespaceOnly(cert)) {
                securityContext.addCertToTrustStore(cert);
            }
        }

        if (!StringUtils.isNullOrWhitespaceOnly(clientCert)
            && !StringUtils.isNullOrWhitespaceOnly(clientPrivateKey)) {
            securityContext.addMTlsCerts(clientCert, clientPrivateKey);
        }

        // NOTE TrustManagers must be created AFTER adding all certificates to KeyStore.
        TrustManager[] trustManagers = getTrustedManagers(securityContext, selfSignedCert);
        return securityContext.getSslContext(trustManagers);
    }

    private static TrustManager[] getTrustedManagers(
        SecurityContext securityContext,
        boolean selfSignedCert) {

        TrustManager[] trustManagers = securityContext.getTrustManagers();

        if (selfSignedCert) {
            return wrapWithSelfSignedManagers(trustManagers).toArray(new TrustManager[0]);
        } else {
            return trustManagers;
        }
    }

    private static TrustManager[] getTrustedManagers() {

        return new TrustManager[]{
                new X509TrustManager() {
                    @Override
                    public void checkClientTrusted(java.security.cert.X509Certificate[] chain, String authType) {
                    }

                    @Override
                    public void checkServerTrusted(java.security.cert.X509Certificate[] chain, String authType) {
                    }

                    @Override
                    public java.security.cert.X509Certificate[] getAcceptedIssuers() {
                        return new java.security.cert.X509Certificate[]{};
                    }
                }
        };
    }

    private static List<TrustManager> wrapWithSelfSignedManagers(TrustManager[] trustManagers) {
        log.warn("Creating Trust Managers for self-signed certificates - not Recommended. "
            + "Use [" + HttpConnectorConfigConstants.SERVER_TRUSTED_CERT + "] "
            + "connector property to add certificated as trusted.");

        List<TrustManager> selfSignedManagers = new ArrayList<>(trustManagers.length);
        for (TrustManager trustManager : trustManagers) {
            selfSignedManagers.add(new SelfSignedTrustManager((X509TrustManager) trustManager));
        }
        return selfSignedManagers;
    }

    /**
     * Creates a {@link SecurityContext} with empty {@link java.security.KeyStore} or loaded from
     * file.
     *
     * @param properties Properties for creating {@link SecurityContext}
     * @return new {@link SecurityContext} instance.
     */
    private static SecurityContext createSecurityContext(Properties properties) {

        String keyStorePath =
            properties.getProperty(HttpConnectorConfigConstants.KEY_STORE_PATH, "");

        if (StringUtils.isNullOrWhitespaceOnly(keyStorePath)) {
            return SecurityContext.create();
        } else {
            char[] storePassword =
                properties.getProperty(HttpConnectorConfigConstants.KEY_STORE_PASSWORD, "")
                    .toCharArray();
            if (storePassword.length == 0) {
                throw new RuntimeException("Missing password for provided KeyStore");
            }
            return SecurityContext.createFromKeyStore(keyStorePath, storePassword);
        }
    }



    /**
     * description 忽略https证书验证
     */
    private static HostnameVerifier getHostnameVerifier() {
        HostnameVerifier hostnameVerifier = new HostnameVerifier() {

            @Override
            public boolean verify(String s, SSLSession sslSession) {
                return true;
            }
        };
        return hostnameVerifier;
    }
}
