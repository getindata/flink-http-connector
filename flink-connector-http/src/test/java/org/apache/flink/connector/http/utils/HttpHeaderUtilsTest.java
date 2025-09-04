/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.http.utils;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.http.preprocessor.HeaderPreprocessor;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.apache.flink.connector.http.table.lookup.HttpLookupConnectorOptions.SOURCE_LOOKUP_OIDC_AUTH_TOKEN_ENDPOINT_URL;
import static org.apache.flink.connector.http.table.lookup.HttpLookupConnectorOptions.SOURCE_LOOKUP_OIDC_AUTH_TOKEN_EXPIRY_REDUCTION;
import static org.apache.flink.connector.http.table.lookup.HttpLookupConnectorOptions.SOURCE_LOOKUP_OIDC_AUTH_TOKEN_REQUEST;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

/** tests for {@link HttpHeaderUtils}. */
public class HttpHeaderUtilsTest {
    @Test
    void shouldCreateOIDCHeaderPreprocessorTest() {
        Configuration configuration = new Configuration();
        HeaderPreprocessor headerPreprocessor =
                HttpHeaderUtils.createOIDCHeaderPreprocessor(configuration);
        assertThat(headerPreprocessor).isNull();
        configuration.setString(SOURCE_LOOKUP_OIDC_AUTH_TOKEN_ENDPOINT_URL.key(), "http://aaa");
        configuration.setString(SOURCE_LOOKUP_OIDC_AUTH_TOKEN_REQUEST.key(), "ccc");
        configuration.set(SOURCE_LOOKUP_OIDC_AUTH_TOKEN_EXPIRY_REDUCTION, Duration.ofSeconds(1));
        headerPreprocessor = HttpHeaderUtils.createOIDCHeaderPreprocessor(configuration);
        assertThat(headerPreprocessor).isNotNull();
    }
}
