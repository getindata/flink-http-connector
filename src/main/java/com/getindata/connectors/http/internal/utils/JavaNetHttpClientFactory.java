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

import org.apache.flink.util.StringUtils;

import com.getindata.connectors.http.internal.config.HttpConnectorConfigConstants;
import com.getindata.connectors.http.internal.security.SecurityContext;
import com.getindata.connectors.http.internal.security.SelfSignedTrustManager;

public class JavaNetHttpClientFactory {

    public static HttpClient createClient(Properties properties) {

        SecurityContext securityContext = SecurityContext.contextForLocalStore();

        boolean selfSignedCert = Boolean.parseBoolean(
            properties.getProperty(HttpConnectorConfigConstants.SELF_SIGNED_CERT, "false"));

        String caRootPath = properties.getProperty(HttpConnectorConfigConstants.CA_CERT, "");

        Builder httpClientBuilder = HttpClient.newBuilder()
            .followRedirects(Redirect.NORMAL);

        if (!StringUtils.isNullOrWhitespaceOnly(caRootPath)) {
            securityContext.addCertToTrustStore(caRootPath);
            SSLContext sslContext = securityContext
                .getSslContext(securityContext.getTrustManagers());
            httpClientBuilder.sslContext(sslContext);
        }

        if (selfSignedCert) {
            TrustManager[] trustManagers = securityContext.getTrustManagers();
            List<TrustManager> selfSignedManagers = new ArrayList<>(trustManagers.length);
            for (TrustManager trustManager : trustManagers) {
                selfSignedManagers.add(new SelfSignedTrustManager((X509TrustManager) trustManager));
            }

            SSLContext sslContext = securityContext.getSslContext(selfSignedManagers);

            httpClientBuilder.sslContext(sslContext);
        }

        return httpClientBuilder.build();
    }

}
