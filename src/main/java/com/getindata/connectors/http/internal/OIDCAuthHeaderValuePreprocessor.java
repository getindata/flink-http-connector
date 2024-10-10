package com.getindata.connectors.http.internal;

import java.net.http.HttpClient;
import java.time.Duration;
import java.util.Optional;

import lombok.extern.slf4j.Slf4j;

import com.getindata.connectors.http.internal.auth.OidcAccessTokenManager;

/**
 * Header processor for HTTP OIDC Authentication mechanism.
 */
@Slf4j
public class OIDCAuthHeaderValuePreprocessor implements HeaderValuePreprocessor {


    private final String oidcAuthURL;
    private final String oidcTokenRequest;
    private Duration oidcExpiryReduction = Duration.ofSeconds(1);
    /**
     * Add the access token to the request using OidcAuth authenticate method that
     * gives us a valid access token.
     * @param oidcAuthURL  OIDC token endpoint
     * @param oidcTokenRequest  OIDC Token Request
     * @param oidcExpiryReduction OIDC token expiry reduction
     */

    public OIDCAuthHeaderValuePreprocessor(String oidcAuthURL,
                                           String oidcTokenRequest,
                                           Optional<Duration> oidcExpiryReduction) {
        this.oidcAuthURL = oidcAuthURL;
        this.oidcTokenRequest = oidcTokenRequest;
        if (oidcExpiryReduction.isPresent()) {
            this.oidcExpiryReduction = oidcExpiryReduction.get();
        }
    }

    @Override
    public String preprocessHeaderValue(String rawValue) {
        OidcAccessTokenManager auth = new OidcAccessTokenManager(
                HttpClient.newBuilder().build(),
                oidcTokenRequest,
                oidcAuthURL,
                oidcExpiryReduction
        );
        // apply the OIDC authentication by adding the dynamically calculated header value.
        return "BEARER " + auth.authenticate();
    }
}
