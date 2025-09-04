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

package org.apache.flink.connector.http.table.lookup;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link LookupQueryInfo} . */
class LookupQueryInfoTest {

    private LookupQueryInfo lookupQueryInfo;

    @Test
    public void testConfiguredLookupQuery() {
        String lookupQuery = "{\"param1\": \"value1\"}";
        Map<String, String> bodyBasedUrlQueryParameters = Map.of("key1", "value1");

        lookupQueryInfo = new LookupQueryInfo(lookupQuery, bodyBasedUrlQueryParameters, null);

        assertThat(lookupQueryInfo.hasLookupQuery()).isTrue();
        assertThat(lookupQueryInfo.getLookupQuery()).isEqualTo(lookupQuery);
        assertThat(lookupQueryInfo.hasBodyBasedUrlQueryParameters()).isTrue();
        assertThat(lookupQueryInfo.getBodyBasedUrlQueryParameters()).isEqualTo("key1=value1");
    }

    @Test
    public void testEmptyLookupQueryInfo() {
        lookupQueryInfo = new LookupQueryInfo(null, null, null);

        assertThat(lookupQueryInfo.hasLookupQuery()).isFalse();
        assertThat(lookupQueryInfo.hasBodyBasedUrlQueryParameters()).isFalse();
        assertThat(lookupQueryInfo.getLookupQuery()).isEqualTo("");
        assertThat(lookupQueryInfo.getBodyBasedUrlQueryParameters()).isEqualTo("");
    }

    @Test
    public void test1BodyParam() {
        Map<String, String> bodyBasedUrlQueryParameters = Map.of("key1", "value1");

        lookupQueryInfo = new LookupQueryInfo(null, bodyBasedUrlQueryParameters, null);

        assertThat(lookupQueryInfo.hasLookupQuery()).isFalse();
        assertThat(lookupQueryInfo.hasBodyBasedUrlQueryParameters()).isTrue();
        assertThat(lookupQueryInfo.getBodyBasedUrlQueryParameters()).isEqualTo("key1=value1");
    }

    @Test
    public void test1PathParam() {
        Map<String, String> pathBasedUrlPathParameters = Map.of("key1", "value1");

        lookupQueryInfo =
                new LookupQueryInfo("http://service/{key1}", null, pathBasedUrlPathParameters);

        assertThat(lookupQueryInfo.hasLookupQuery()).isTrue();
        assertThat(lookupQueryInfo.hasPathBasedUrlParameters()).isTrue();
        assertThat(lookupQueryInfo.getPathBasedUrlParameters())
                .isEqualTo(pathBasedUrlPathParameters);
    }

    @Test
    public void test2Path2BodyParams() {
        Map<String, String> pathBasedUrlPathParameters = Map.of("key1", "value1", "key2", "value2");
        Map<String, String> bodyBasedQueryParameters = Map.of("key3", "value3", "key4", "value4");

        lookupQueryInfo =
                new LookupQueryInfo(null, bodyBasedQueryParameters, pathBasedUrlPathParameters);

        assertThat(lookupQueryInfo.hasLookupQuery()).isFalse();
        assertThat(lookupQueryInfo.hasPathBasedUrlParameters()).isTrue();
        assertThat(lookupQueryInfo.getPathBasedUrlParameters())
                .isEqualTo(pathBasedUrlPathParameters);
        assertThat(lookupQueryInfo.hasBodyBasedUrlQueryParameters()).isTrue();
        assertThat(lookupQueryInfo.getBodyBasedUrlQueryParameters())
                .isEqualTo("key3=value3&key4=value4");
    }

    @Test
    public void test2PathParams() {
        Map<String, String> pathBasedUrlPathParameters = Map.of("key1", "value1", "key2", "value2");

        lookupQueryInfo = new LookupQueryInfo(null, null, pathBasedUrlPathParameters);

        assertThat(lookupQueryInfo.hasLookupQuery()).isFalse();
        assertThat(lookupQueryInfo.hasPathBasedUrlParameters()).isTrue();
        assertThat(lookupQueryInfo.getPathBasedUrlParameters())
                .isEqualTo(pathBasedUrlPathParameters);
    }
}
