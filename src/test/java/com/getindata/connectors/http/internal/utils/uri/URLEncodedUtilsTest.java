package com.getindata.connectors.http.internal.utils.uri;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.List;

import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;

class URLEncodedUtilsTest {

    @Test
    public void testFormatSegments() {
        String segments =
            URLEncodedUtils.formatSegments(
                List.of("segmentOne", "segmentTwo", "segmentThree"),
                StandardCharsets.UTF_8
            );

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

        String segments =
            URLEncodedUtils.formatSegments(segmentList, StandardCharsets.UTF_8);

        assertThat(segments).isEqualTo("/null");
    }

    @Test
    public void testNullParse() {
        List<NameValuePair> components =
            URLEncodedUtils.parse(null, StandardCharsets.UTF_8);

        assertThat(components).isEmpty();
    }

    @Test
    public void testBlankParse() {
        List<NameValuePair> components =
            URLEncodedUtils.parse(" ", StandardCharsets.UTF_8);

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
        List<String> segments = URLEncodedUtils.parsePathSegments(
            "/hello//%world",
            StandardCharsets.UTF_8
        );

        assertThat(segments).isEqualTo(List.of("hello", "", "%world"));
    }

    @Test
    public void testParseSegmentsComplex() {
        List<String> segments = URLEncodedUtils.parsePathSegments(
            "/hello//world?q=what+%25+is+in+HTTP",
            StandardCharsets.UTF_8
        );

        assertThat(segments).isEqualTo(List.of("hello", "", "world?q=what+%+is+in+HTTP"));
    }
}
