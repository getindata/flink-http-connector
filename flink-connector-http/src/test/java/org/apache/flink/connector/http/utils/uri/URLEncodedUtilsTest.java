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

package org.apache.flink.connector.http.utils.uri;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** test for {@link URLEncodedUtils}. */
class URLEncodedUtilsTest {

    @Test
    public void testFormatSegments() {
        String segments =
                URLEncodedUtils.formatSegments(
                        List.of("segmentOne", "segmentTwo", "segmentThree"),
                        StandardCharsets.UTF_8);

        assertThat(segments).isEqualTo("/segmentOne/segmentTwo/segmentThree");
    }

    @Test
    public void testFormatEmptySegments() {
        String segments =
                URLEncodedUtils.formatSegments(Collections.emptyList(), StandardCharsets.UTF_8);

        assertThat(segments).isEqualTo("");
    }

    @Test
    public void testFormatNullSegments() {
        List<String> segmentList = new ArrayList<>();
        segmentList.add(null);

        String segments = URLEncodedUtils.formatSegments(segmentList, StandardCharsets.UTF_8);

        assertThat(segments).isEqualTo("/null");
    }

    @Test
    public void testNullParse() {
        List<NameValuePair> components = URLEncodedUtils.parse(null, StandardCharsets.UTF_8);

        assertThat(components).isEmpty();
    }

    @Test
    public void testBlankParse() {
        List<NameValuePair> components = URLEncodedUtils.parse(" ", StandardCharsets.UTF_8);

        assertThat(components).isEmpty();
    }

    @Test
    public void testParse() {
        List<NameValuePair> components =
                URLEncodedUtils.parse("Hello Me=val1", StandardCharsets.UTF_8);

        assertThat(components).hasSize(1);
        assertThat(components.get(0)).isEqualTo(new NameValuePair("Hello Me", "val1"));
    }

    @Test
    public void testSplitNoSegments() {
        List<String> segments = URLEncodedUtils.splitSegments("", new BitSet());

        assertThat(segments).isEmpty();
    }

    @Test
    public void testSplitSegments() {
        List<String> segments = URLEncodedUtils.splitSegments("segments", new BitSet());

        assertThat(segments).hasSize(1);
        assertThat(segments.get(0)).isEqualTo("segments");
    }

    @Test
    public void testSplitEmptySegments() {

        List<String> segments = URLEncodedUtils.splitSegments("/hello//world", new BitSet());

        assertThat(segments).hasSize(1);
        assertThat(segments.get(0)).isEqualTo("/hello//world");
    }

    @Test
    public void testParseSegments() {
        List<String> segments =
                URLEncodedUtils.parsePathSegments("/hello//%world", StandardCharsets.UTF_8);

        assertThat(segments).isEqualTo(List.of("hello", "", "%world"));
    }

    @Test
    public void testParseSegmentsComplex() {
        List<String> segments =
                URLEncodedUtils.parsePathSegments(
                        "/hello//world?q=what+%25+is+in+HTTP", StandardCharsets.UTF_8);

        assertThat(segments).isEqualTo(List.of("hello", "", "world?q=what+%+is+in+HTTP"));
    }
}
