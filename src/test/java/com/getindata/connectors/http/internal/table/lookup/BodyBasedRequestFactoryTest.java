package com.getindata.connectors.http.internal.table.lookup;

import java.net.URI;
import java.util.Collection;
import java.util.Map;

import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.testcontainers.shaded.com.google.common.collect.ImmutableList;
import static org.assertj.core.api.Assertions.assertThat;

public class BodyBasedRequestFactoryTest {

    @ParameterizedTest
    @MethodSource("configProvider")
    void testconstructUri(TestSpec testSpec) throws Exception {
        LookupQueryInfo lookupQueryInfo = new LookupQueryInfo(testSpec.url,
                testSpec.bodyBasedUrlQueryParams,
                testSpec.pathBasedUrlParams);
        HttpLookupConfig httpLookupConfig = HttpLookupConfig.builder()
                .lookupMethod(testSpec.lookupMethod)
                .url(testSpec.url)
                .useAsync(false)
                .build();
        BodyBasedRequestFactory bodyBasedRequestFactory =
                new BodyBasedRequestFactory("test", null, null, httpLookupConfig);

        URI uri = bodyBasedRequestFactory.constructUri(lookupQueryInfo);
        assertThat(uri.toString()).isEqualTo(testSpec.expected);
    }

    private static class TestSpec {

        Map<String, String> bodyBasedUrlQueryParams;
        Map<String, String> pathBasedUrlParams;
        String url;
        String lookupMethod;
        String expected;

        private TestSpec(Map<String, String> bodyBasedUrlQueryParams,
                         Map<String, String> pathBasedUrlParams,
                         String url,
                         String lookupMethod,
                         String expected) {
            this.bodyBasedUrlQueryParams = bodyBasedUrlQueryParams;
            this.pathBasedUrlParams = pathBasedUrlParams;
            this.url = url;
            this.lookupMethod = lookupMethod;
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
                    + ", lookupMethod="
                    + lookupMethod
                    + ", expected="
                    + expected
                    + '}';
        }
    }

    static Collection<TestSpec> configProvider() {
        return ImmutableList.<TestSpec>builder()
                .addAll(getTestSpecs("GET"))
                .addAll(getTestSpecs("POST"))
                .build();
    }

    @NotNull
    private static ImmutableList<TestSpec> getTestSpecs(String lookupMethod) {
        return ImmutableList.of(
                // 1 path param
                new TestSpec(
                        null,
                        Map. of("param1", "value1"),
                        "http://service/{param1}",
                        lookupMethod,
                        "http://service/value1"),
                // 2 path param
                new TestSpec(
                        null,
                        Map. of("param1", "value1", "param2", "value2"),
                        "http://service/{param1}/param2/{param2}",
                        lookupMethod,
                        "http://service/value1/param2/value2"),
                // 1 query param
                new TestSpec(
                        Map. of("param3", "value3"),
                        null,
                        "http://service",
                        lookupMethod,
                        "http://service?param3=value3"),
                // 1 query param with a parameter on base url
                new TestSpec(
                        Map. of("param3", "value3"),
                        null,
                        "http://service?extrakey=extravalue",
                        lookupMethod,
                        "http://service?extrakey=extravalue&param3=value3"),
                // 2 query params
                new TestSpec(
                        Map. of("param3", "value3", "param4", "value4"),
                        null,
                        "http://service",
                        lookupMethod,
                        "http://service?param3=value3&param4=value4"),
                // 2 query params and 2 path params
                new TestSpec(
                        Map. of("param3", "value3", "param4", "value4"),
                        Map. of("param1", "value1", "param2", "value2"),
                        "http://service/{param1}/param2/{param2}",
                        lookupMethod,
                        "http://service/value1/param2/value2?param3=value3&param4=value4")
        );
    }
}
