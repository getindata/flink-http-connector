package com.getindata.connectors.http.internal.utils;

import java.net.http.HttpClient;
import java.net.http.HttpClient.Builder;
import java.net.http.HttpClient.Redirect;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

import com.getindata.connectors.http.internal.config.HttpConnectorConfigConstants;
import com.getindata.connectors.http.internal.security.SelfSignedTrustManager;
import com.getindata.connectors.http.internal.security.SslContextFactory;

public class JavaNetHttpClientFactory {

    public static HttpClient createClient(Properties properties) {

        boolean selfSignedCert = Boolean.parseBoolean(
            properties.getProperty(HttpConnectorConfigConstants.SELF_SIGNED_CERT, "false"));

        Builder httpClientBuilder = HttpClient.newBuilder()
            .followRedirects(Redirect.NORMAL);

        if (selfSignedCert) {
            TrustManager[] trustManagers = SslContextFactory.getTrustManagers();
            List<TrustManager> selfSignedManagers = new ArrayList<>(trustManagers.length);
            for (TrustManager trustManager : trustManagers) {
                selfSignedManagers.add(new SelfSignedTrustManager((X509TrustManager) trustManager));
            }

            SSLContext sslContext = SslContextFactory.getSslContext(null,
                selfSignedManagers.toArray(new TrustManager[0]));

            httpClientBuilder.sslContext(sslContext);
        }

        return httpClientBuilder.build();
    }

}
