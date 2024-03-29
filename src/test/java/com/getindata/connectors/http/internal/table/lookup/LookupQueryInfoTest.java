package com.getindata.connectors.http.internal.table.lookup;

import java.net.URI;
import java.util.Collection;
import java.util.Map;

import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.testcontainers.shaded.com.google.common.collect.ImmutableList;
import static org.assertj.core.api.Assertions.assertThat;

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
    public void testBodyBasedUrlQueryParams() {
        Map<String, String> bodyBasedUrlQueryParameters = Map.of("key1", "value1");

        lookupQueryInfo = new LookupQueryInfo(null, bodyBasedUrlQueryParameters, null);

        assertThat(lookupQueryInfo.hasLookupQuery()).isFalse();
        assertThat(lookupQueryInfo.hasBodyBasedUrlQueryParameters()).isTrue();
        assertThat(lookupQueryInfo.getBodyBasedUrlQueryParameters()).isEqualTo("key1=value1");
    }

    @Test
    public void testPathBasedUrlParams1() {
        Map<String, String> pathBasedUrlPathParameters = Map.of("key1", "value1");

        lookupQueryInfo = new LookupQueryInfo("http://service/{key1}",
                null, pathBasedUrlPathParameters);

        assertThat(lookupQueryInfo.hasLookupQuery()).isTrue();
        assertThat(lookupQueryInfo.hasPathBasedUrlParameters()).isTrue();
        assertThat(lookupQueryInfo.getPathBasedUrlParameters())
                .isEqualTo(pathBasedUrlPathParameters);
        assertThat(lookupQueryInfo.getURI().toString())
                .isEqualTo("http://service/value1");
    }
    @Test
    public void testQueryAndPathBasedUrlParams2() {
        Map<String, String> pathBasedUrlPathParameters =
                Map.of("key1", "value1", "key2", "value2");
        Map<String, String> bodyBasedQueryParameters =
                Map.of("key3", "value3", "key4", "value4");

        lookupQueryInfo = new LookupQueryInfo(null,
                bodyBasedQueryParameters, pathBasedUrlPathParameters);

        assertThat(lookupQueryInfo.hasLookupQuery()).isFalse();
        assertThat(lookupQueryInfo.hasPathBasedUrlParameters()).isTrue();
        assertThat(lookupQueryInfo.getPathBasedUrlParameters())
                .isEqualTo(pathBasedUrlPathParameters);
        assertThat(lookupQueryInfo.hasBodyBasedUrlQueryParameters())
                .isTrue();
        assertThat(lookupQueryInfo.getBodyBasedUrlQueryParameters())
                .isEqualTo("key3=value3&key4=value4");

    }

    @Test
    public void testPathBasedUrlParams2() {
        Map<String, String> pathBasedUrlPathParameters = Map.of("key1", "value1", "key2", "value2");

        lookupQueryInfo = new LookupQueryInfo(null, null, pathBasedUrlPathParameters);

        assertThat(lookupQueryInfo.hasLookupQuery()).isFalse();
        assertThat(lookupQueryInfo.hasPathBasedUrlParameters()).isTrue();
        assertThat(lookupQueryInfo.getPathBasedUrlParameters())
                .isEqualTo(pathBasedUrlPathParameters);
    }

    @ParameterizedTest
    @MethodSource("configProviderForGetURI")
    void testGetUri(LookupQueryInfoTest.TestSpec testSpec) throws Exception {
        LookupQueryInfo lookupQueryInfo = new LookupQueryInfo(testSpec.url,
                testSpec.bodyBasedUrlQueryParams,
                testSpec.pathBasedUrlParams);
        URI uri = lookupQueryInfo.getURI();
        assertThat(uri.toString()).isEqualTo(testSpec.expected);
    }

    private static class TestSpec {

        Map<String, String> bodyBasedUrlQueryParams;
        Map<String, String> pathBasedUrlParams;
        String url;
        String expected;

        private TestSpec(Map<String, String> bodyBasedUrlQueryParams,
                         Map<String, String> pathBasedUrlParams,
                         String url,
                         String expected) {
            this.bodyBasedUrlQueryParams = bodyBasedUrlQueryParams;
            this.pathBasedUrlParams = pathBasedUrlParams;
            this.url = url;
            this.expected = expected;
        }

        @Override
        public String toString() {
            return "TestSpec{"
                    + "bodyBasedUrlQueryParams="
                    + bodyBasedUrlQueryParams
                    + ", pathBasedUrlParams="
                    + pathBasedUrlParams
                    + ", url="
                    + url
                    + ", expected="
                    + expected
                    + '}';
        }
    }

    static Collection<TestSpec> configProviderForGetURI() {
        return ImmutableList.<TestSpec>builder()
                .addAll(getTestSpecs())
                .build();
    }

    @NotNull
    private static ImmutableList<TestSpec> getTestSpecs() {
        return ImmutableList.of(
                // 1 path param
                new TestSpec(
                        null,
                        Map. of("param1", "value1"),
                        "http://service/{param1}",
                        "http://service/value1"),
                // 2 path param
                new TestSpec(
                        null,
                        Map. of("param1", "value1", "param2", "value2"),
                        "http://service/{param1}/param2/{param2}",
                        "http://service/value1/param2/value2"),
                // 1 query param
                new TestSpec(
                        Map. of("param3", "value3"),
                        null,
                        "http://service",
                        "http://service?param3=value3"),
                // 2 query params
                new TestSpec(
                        Map. of("param3", "value3", "param4", "value4"),
                        null,
                        "http://service",
                        "http://service?param3=value3&param4=value4"),
                // 2 query params and 2 path params
                new TestSpec(
                        Map. of("param3", "value3", "param4", "value4"),
                        Map. of("param1", "value1", "param2", "value2"),
                        "http://service/{param1}/param2/{param2}",
                        "http://service/value1/param2/value2?param3=value3&param4=value4")
        );
    }


}
