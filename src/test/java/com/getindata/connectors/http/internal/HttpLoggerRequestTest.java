package com.getindata.connectors.http.internal;

import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpRequest;
import java.util.Collection;
import java.util.Properties;

import com.google.common.collect.ImmutableList;
import lombok.Data;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import static org.assertj.core.api.Assertions.assertThat;

import com.getindata.connectors.http.internal.config.HttpConnectorConfigConstants;

public class HttpLoggerRequestTest {
    @Test
    void testCreateStringForBody() {
    }

    @ParameterizedTest
    @MethodSource("configProvider")
    void testCreateStringForRequest(HttpLoggerRequestTest.TestSpec testSpec) throws URISyntaxException {

        Properties properties = new Properties();
        if (testSpec.httpLoggingLevelType != null) {
            properties.put(HttpConnectorConfigConstants.HTTP_LOGGING_LEVEL, testSpec.getHttpLoggingLevelType().name());
        }

        HttpLogger httpLogger = HttpLogger.getHttpLogger(properties);
        URI uri = new URI("http://aaa");
        HttpRequest.Builder httpRequestBuilder = HttpRequest.newBuilder().uri(uri);
        if (testSpec.isHasHeaders()) {
            httpRequestBuilder.headers("bbb","ccc","bbb","ddd","eee","fff");
        }
        if (testSpec.getMethod().equals("POST")) {
            if (testSpec.isHasBody()) {
                httpRequestBuilder.method("POST", HttpRequest.BodyPublishers.ofString("my body"));
            } else {
                httpRequestBuilder.method("POST", HttpRequest.BodyPublishers.noBody());
            }
        }
        assertThat(httpLogger.createStringForRequest(httpRequestBuilder.build()))
            .isEqualTo(testSpec.getExpectedOutput());

    }

    @Data
    static class TestSpec {
        final HttpLoggingLevelType httpLoggingLevelType;
        final String method;
        final boolean hasBody;
        final boolean hasHeaders;
        final String expectedOutput;
    }

    static Collection<TestSpec> configProvider() {
        return ImmutableList.<TestSpec>builder()
            .addAll(getTestSpecs())
            .build();
    }

    private static ImmutableList<TestSpec> getTestSpecs() {
        return ImmutableList.of(
            // GET no headers
            new TestSpec(null, "GET", false, false,
                "HTTP GET Request: URL: http://aaa, Headers: None"),
            new TestSpec(HttpLoggingLevelType.MIN, "GET", false, false,
                "HTTP GET Request: URL: http://aaa, Headers: None"),
            new TestSpec(HttpLoggingLevelType.REQRESPONSE, "GET", false, false,
                "HTTP GET Request: URL: http://aaa, Headers: None"),
            new TestSpec(HttpLoggingLevelType.MAX, "GET", false, false,
                "HTTP GET Request: URL: http://aaa, Headers: None"),
            // GET with headers
            new TestSpec(null, "GET", false, true,
                "HTTP GET Request: URL: http://aaa, Headers: ***"),
            new TestSpec(HttpLoggingLevelType.MIN, "GET", false, true,
                "HTTP GET Request: URL: http://aaa, Headers: ***"),
            new TestSpec(HttpLoggingLevelType.REQRESPONSE, "GET", false, true,
                "HTTP GET Request: URL: http://aaa, Headers: ***"),
            new TestSpec(HttpLoggingLevelType.MAX, "GET", false, true,
                "HTTP GET Request: URL: http://aaa, Headers: bbb:[ccc;ddd];eee:[fff]"),

            // POST no headers
            new TestSpec(null, "POST", false, false,
            "HTTP POST Request: URL: http://aaa, Headers: None"),
            new TestSpec(HttpLoggingLevelType.MIN, "POST", false, false,
                "HTTP POST Request: URL: http://aaa, Headers: None"),
            new TestSpec(HttpLoggingLevelType.REQRESPONSE, "POST", false, false,
                "HTTP POST Request: URL: http://aaa, Headers: None"),
            new TestSpec(HttpLoggingLevelType.MAX, "POST", false, false,
                "HTTP POST Request: URL: http://aaa, Headers: None"),
            // POST with headers
            new TestSpec(null, "POST", false, true,
                "HTTP POST Request: URL: http://aaa, Headers: ***"),
            new TestSpec(HttpLoggingLevelType.MIN, "POST", false, true,
                "HTTP POST Request: URL: http://aaa, Headers: ***"),
            new TestSpec(HttpLoggingLevelType.REQRESPONSE, "POST", false, true,
                "HTTP POST Request: URL: http://aaa, Headers: ***"),
            new TestSpec(HttpLoggingLevelType.MAX, "POST", false, true,
                "HTTP POST Request: URL: http://aaa, Headers: bbb:[ccc;ddd];eee:[fff]"),

            // POST no headers with body
            new TestSpec(null, "POST", true, false,
                "HTTP POST Request: URL: http://aaa, Headers: None"),
            new TestSpec(HttpLoggingLevelType.MIN, "POST", true, false,
                "HTTP POST Request: URL: http://aaa, Headers: None"),
            new TestSpec(HttpLoggingLevelType.REQRESPONSE, "POST", true, false,
                "HTTP POST Request: URL: http://aaa, Headers: None"),
            new TestSpec(HttpLoggingLevelType.MAX, "POST", true, false,
                "HTTP POST Request: URL: http://aaa, Headers: None"),
            // POST with headers with body
            new TestSpec(null, "POST", true, true,
                "HTTP POST Request: URL: http://aaa, Headers: ***"),
            new TestSpec(HttpLoggingLevelType.MIN, "POST", true, true,
                "HTTP POST Request: URL: http://aaa, Headers: ***"),
            new TestSpec(HttpLoggingLevelType.REQRESPONSE, "POST", true, true,
                "HTTP POST Request: URL: http://aaa, Headers: ***"),
            new TestSpec(HttpLoggingLevelType.MAX, "POST", true, true,
                "HTTP POST Request: URL: http://aaa, Headers: bbb:[ccc;ddd];eee:[fff]")

        );
    }

}
