/*
 * ====================================================================
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 * ====================================================================
 *
 * This software consists of voluntary contributions made by many
 * individuals on behalf of the Apache Software Foundation.  For more
 * information on the Apache Software Foundation, please see
 * <http://www.apache.org/>.
 *
 * ============================= NOTE =================================
 * This code has been copied from
 * https://github.com/apache/httpcomponents-client/tree/rel/v4.5.13
 * and it was changed to use in this project.
 * ====================================================================
 */

package com.getindata.connectors.http.internal.utils.uri;

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import org.apache.flink.util.StringUtils;

/**
 * Builder for {@link URI} instances.
 * This class is based on {@code org.apache.httpcomponents.httpclient#URIBuilder} version 4.5.13.
 */
public class URIBuilder {

    private String scheme;

    private String encodedSchemeSpecificPart;

    private String encodedAuthority;

    private final Charset charset;

    private String encodedPath;

    private List<NameValuePair> queryParams;

    private String encodedFragment;

    private String encodedQuery;

    /**
     * Construct an instance from the string which must be a valid URI.
     *
     * @param string a valid URI in string form
     * @throws URISyntaxException if the input is not a valid URI
     */
    public URIBuilder(final String string) throws URISyntaxException {
        this(new URI(string), null);
    }

    public URIBuilder(URI uri) {
        this(uri, null);
    }

    /**
     * Construct an instance from the provided URI.
     *
     * @param uri
     */
    public URIBuilder(final URI uri, final Charset charset) {
        super();
        this.charset = charset;
        digestURI(uri);
    }

    /**
     * Adds parameter to URI query. The parameter name and value are expected to be unescaped and
     * may contain non ASCII characters.
     * <p>
     * Please note query parameters and custom query component are mutually exclusive. This method
     * will remove custom query if present.
     * </p>
     */
    public URIBuilder addParameter(final String param, final String value) {
        if (this.queryParams == null) {
            this.queryParams = new ArrayList<>();
        }
        this.queryParams.add(new NameValuePair(param, value));
        this.encodedQuery = null;
        this.encodedSchemeSpecificPart = null;
        return this;
    }

    /**
     * Builds a {@link URI} instance.
     */
    public URI build() throws URISyntaxException {
        return new URI(buildString());
    }

    private static String normalizePath(final String path, final boolean relative) {
        if (StringUtils.isNullOrWhitespaceOnly(path)) {
            return "";
        }

        return path;
    }

    private List<NameValuePair> parseQuery(String query, Charset charset) {
        return query != null && !query.isEmpty() ? URLEncodedUtils.parse(query, charset) : null;
    }

    private String buildString() {
        final StringBuilder sb = new StringBuilder();
        if (this.scheme != null) {
            sb.append(this.scheme).append(':');
        }
        if (this.encodedSchemeSpecificPart != null) {
            sb.append(this.encodedSchemeSpecificPart);
        } else {
            if (this.encodedAuthority != null) {
                sb.append("//").append(this.encodedAuthority);
            }

            if (this.encodedPath != null) {
                sb.append(normalizePath(this.encodedPath, sb.length() == 0));
            }

            if (this.encodedQuery != null) {
                sb.append("?").append(this.encodedQuery);
            } else if (this.queryParams != null && !this.queryParams.isEmpty()) {
                sb.append("?").append(encodeUrlForm(this.queryParams));
            }
        }
        if (this.encodedFragment != null) {
            sb.append("#").append(this.encodedFragment);
        }

        return sb.toString();
    }

    private void digestURI(final URI uri) {
        this.scheme = uri.getScheme();
        this.encodedSchemeSpecificPart = uri.getRawSchemeSpecificPart();
        this.encodedAuthority = uri.getRawAuthority();
        this.encodedPath = uri.getRawPath();
        this.queryParams =
            parseQuery(
                uri.getRawQuery(),
                this.charset != null ? this.charset : StandardCharsets.UTF_8
            );
        this.encodedFragment = uri.getRawFragment();
        this.encodedQuery = uri.getRawQuery();
    }

    private String encodeUrlForm(final List<NameValuePair> params) {
        return URLEncodedUtils.format(
            params, this.charset != null ? this.charset : StandardCharsets.UTF_8
        );
    }
}

