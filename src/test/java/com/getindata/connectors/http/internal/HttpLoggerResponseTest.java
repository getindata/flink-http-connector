package com.getindata.connectors.http.internal;

import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpHeaders;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import javax.net.ssl.SSLSession;

import com.google.common.collect.ImmutableList;
import lombok.Data;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import static org.assertj.core.api.Assertions.assertThat;

import com.getindata.connectors.http.internal.config.HttpConnectorConfigConstants;

public class HttpLoggerResponseTest {
    @ParameterizedTest
    @MethodSource("configProvider")
    void testCreateStringForResponse(HttpLoggerResponseTest.TestSpec testSpec) throws URISyntaxException {

        Properties properties = new Properties();
        if (testSpec.httpLoggingLevelType != null) {
            properties.put(HttpConnectorConfigConstants.HTTP_LOGGING_LEVEL, testSpec.getHttpLoggingLevelType().name());
        }

        HttpLogger httpLogger = HttpLogger.getHttpLogger(properties);
        URI uri = new URI("http://aaa");
        MockHttpResponse response = new MockHttpResponse();
        response.setStatusCode(testSpec.getStatusCode());
        if (testSpec.method.equals("GET")) {
            response.setRequest(HttpRequest.newBuilder().GET().uri(uri).build());
        } else {
            // dummy request so we can populate the method in the log
            response.setRequest(HttpRequest.newBuilder().POST(HttpRequest.BodyPublishers.noBody()).uri(uri).build());
        }

        if (testSpec.isHasHeaders()) {
            // "bbb","ccc","bbb","ddd","eee","fff"
            Map<String, List<String>> headersMap = new HashMap<>();
            headersMap.put("bbb", List.of("ccc", "ddd"));
            headersMap.put("eee", List.of("fff"));

            HttpHeaders headers = HttpHeaders.of(headersMap, (name, value) -> true);
            response.setHeaders(headers);
        }

        if (testSpec.isHasBody()) {
            response.setBody("my body");
        }

        assertThat(httpLogger.createStringForResponse(response)).isEqualTo(testSpec.getExpectedOutput());

    }

    @Data
    static class TestSpec {
        final String method;
        final int statusCode;
        final HttpLoggingLevelType httpLoggingLevelType;
        final boolean hasBody;
        final boolean hasHeaders;
        final String expectedOutput;
    }

    static Collection<TestSpec> configProvider() {
        return ImmutableList.<TestSpec>builder()
            .addAll(getTestSpecs("GET", 200))
            .addAll(getTestSpecs("POST", 500))
            .build();
    }

    private static ImmutableList<TestSpec> getTestSpecs(String method, int statusCode) {
        return ImmutableList.of(
              // no headers no body
              new TestSpec(method, statusCode, null,  false, false,
                "HTTP " + method + " Response: URL: http://aaa, "
                    + "Response Headers: None, status code: " + statusCode + ", Response Body: None"),
              new TestSpec(method, statusCode, HttpLoggingLevelType.MIN,  false, false,
                "HTTP " + method + " Response: URL: http://aaa, "
                    + "Response Headers: None, status code: " + statusCode + ", Response Body: None"),
              new TestSpec(method, statusCode, HttpLoggingLevelType.REQRESPONSE,  false, false,
                "HTTP " + method + " Response: URL: http://aaa, "
                    + "Response Headers: None, status code: " + statusCode + ", Response Body: None"),
              new TestSpec(method, statusCode, HttpLoggingLevelType.MAX,  false, false,
                "HTTP " + method + " Response: URL: http://aaa, "
                    + "Response Headers: None, status code: " + statusCode + ", Response Body: None"),
            // with headers

            new TestSpec(method, statusCode, null,  false, true,
                "HTTP " + method + " Response: URL: http://aaa, "
                    + "Response Headers: ***, status code: " + statusCode + ", Response Body: None"),
            new TestSpec(method, statusCode, HttpLoggingLevelType.MIN,  false, true,
                "HTTP " + method + " Response: URL: http://aaa, "
                    + "Response Headers: ***, status code: " + statusCode + ", Response Body: None"),
            new TestSpec(method, statusCode, HttpLoggingLevelType.REQRESPONSE,  false, true,
                "HTTP " + method + " Response: URL: http://aaa, "
                    + "Response Headers: ***, status code: " + statusCode + ", Response Body: None"),
            new TestSpec(method, statusCode, HttpLoggingLevelType.MAX,  false, true,
                "HTTP " + method + " Response: URL: http://aaa, "
                    + "Response Headers: bbb:[ccc;ddd];eee:[fff], status code: " + statusCode + ", "
                    + "Response Body: None"),

            // no headers with body
            new TestSpec(method, statusCode, null,  true, false,
                "HTTP " + method + " Response: URL: http://aaa, "
                    + "Response Headers: None, status code: " + statusCode + ", Response Body: ***"),
            new TestSpec(method, statusCode, HttpLoggingLevelType.MIN,  true, false,
                "HTTP " + method + " Response: URL: http://aaa, "
                    + "Response Headers: None, status code: " + statusCode + ", Response Body: ***"),
            new TestSpec(method, statusCode, HttpLoggingLevelType.REQRESPONSE,  true, false,
                "HTTP " + method + " Response: URL: http://aaa, "
                    + "Response Headers: None, status code: " + statusCode + ", Response Body: my body"),
            new TestSpec(method, statusCode, HttpLoggingLevelType.MAX,  true, false,
                "HTTP " + method + " Response: URL: http://aaa, "
                    + "Response Headers: None, status code: " + statusCode + ", Response Body: my body"),

            // headers with body
            new TestSpec(method, statusCode, null,  true, true,
                "HTTP " + method + " Response: URL: http://aaa, Response Headers: ***"
                  + ", status code: " + statusCode + ", Response Body: ***"),
            new TestSpec(method, statusCode, HttpLoggingLevelType.MIN,  true, true,
                "HTTP " + method + " Response: URL: http://aaa, Response Headers: ***"
                  + ", status code: " + statusCode + ", Response Body: ***"),
            new TestSpec(method, statusCode, HttpLoggingLevelType.REQRESPONSE,  true, true,
                "HTTP " + method + " Response: URL: http://aaa, Response Headers: ***"
                  + ", status code: " + statusCode + ", Response Body: my body"),
            new TestSpec(method, statusCode, HttpLoggingLevelType.MAX,  true, true,
                "HTTP " + method + " Response: URL: http://aaa, "
                  + "Response Headers: bbb:[ccc;ddd];eee:[fff]"
                  + ", status code: " + statusCode + ", Response Body: my body")
        );
    }

    private class MockHttpResponse implements HttpResponse {
        private int statusCode = 0;
        private HttpRequest request = null;
        private HttpHeaders headers = null;
        private String body = null;

        @Override
        public int statusCode() {
            return this.statusCode;
        }

        @Override
        public HttpRequest request() {
            return this.request;
        }

        @Override
        public Optional<HttpResponse> previousResponse() {
            return Optional.empty();
        }

        @Override
        public HttpHeaders headers() {
            return this.headers;
        }

        @Override
        public Object body() {
            return this.body;
        }

        @Override
        public Optional<SSLSession> sslSession() {
            return Optional.empty();
        }

        @Override
        public URI uri() {
            URI uri;
            try {
                uri = new URI("http://aaa");
            } catch (URISyntaxException e) {
                throw new RuntimeException(e);
            }
            return uri;
        }

        @Override
        public HttpClient.Version version() {
            return null;
        }

        public void setStatusCode(int statusCode) {
            this.statusCode = statusCode;
        }

        public void setRequest(HttpRequest request) {
            this.request = request;
        }

        public void setHeaders(HttpHeaders headers) {
            this.headers = headers;
        }

        public void setBody(String body) {
            this.body = body;
        }
    }
}
