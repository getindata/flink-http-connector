package com.getindata.connectors.http.internal.utils.uri;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import static org.assertj.core.api.Assertions.assertThat;

import com.getindata.connectors.http.LookupArg;

class URIBuilderTest {

    private static final String HOST_URL = "http://localhost:8080/client";

    private static final String HOST_URL_WITH_PARAMS = "http://localhost:8080/client?val=1";

    private static final String HOST_URL_WITH_PARAMS_MARK = "http://localhost:8080/client?";

    private static final String HOST_URL_WITH_END = "http://localhost:8080/client/";

    private static final String HOST_URL_NO_PORT = "http://localhost/client";

    private static final String HOST_URL_NO_SCHEMA = "localhost/client";

    private static final String HOST_URL_NO_PATH = "http://localhost:8080";

    private static final String HOST_URL_USER_INFO = "http://userMe@localhost:8080/client";

    private static final String IPv4_URL = "http://127.0.0.1:8080/client";

    private static final String IPv4_URL_NO_PORT = "http://127.0.0.1/client";

    private static final String IPv4_URL_NO_PATH = "http://127.0.0.1";

    private static final String IPv4_URL_USER_INFO = "http://userMe@127.0.0.1/client";

    private static final String IPv4_URL_NO_SCHEMA = "127.0.0.1/client";

    private URIBuilder uriBuilder;

    public static Stream<Arguments> uriArgs() {
        return Stream.of(
            Arguments.of(
                List.of(new LookupArg("id", "1"), new LookupArg("name", "2")),
                "?id=1&name=2"),
            Arguments.of(List.of(new LookupArg("id", ""), new LookupArg("name", "")),
                "?id=&name="),
            Arguments.of(
                List.of(new LookupArg("id", " "), new LookupArg("name", " ")),
                "?id=+&name=+"),
            Arguments.of(
                List.of(new LookupArg("id", null), new LookupArg("name", null)),
                "?id&name"),
            Arguments.of(
                List.of(new LookupArg("id", "1"), new LookupArg("name", "what+%25+is+in+HTTP+URL")),
                "?id=1&name=what%2B%2525%2Bis%2Bin%2BHTTP%2BURL")
        );
    }

    @ParameterizedTest
    @MethodSource("uriArgs")
    public void shouldBuildUriForHost(List<LookupArg> arguments, String expectedUriArgs)
        throws URISyntaxException {
        uriBuilder = new URIBuilder(HOST_URL);

        testUriBuilder(arguments, uriBuilder, HOST_URL, expectedUriArgs);
    }

    @ParameterizedTest
    @MethodSource("uriArgs")
    public void shouldBuildUriForHostNoPort(List<LookupArg> arguments, String expectedUriArgs)
        throws URISyntaxException {
        uriBuilder = new URIBuilder(HOST_URL_NO_PORT);

        testUriBuilder(arguments, uriBuilder, HOST_URL_NO_PORT, expectedUriArgs);
    }

    @ParameterizedTest
    @MethodSource("uriArgs")
    public void shouldBuildUriForIPv4(List<LookupArg> arguments, String expectedUriArgs)
        throws URISyntaxException {
        uriBuilder = new URIBuilder(IPv4_URL);

        testUriBuilder(arguments, uriBuilder, IPv4_URL, expectedUriArgs);
    }

    @ParameterizedTest
    @MethodSource("uriArgs")
    public void shouldBuildUriForIPv4NoPort(List<LookupArg> arguments, String expectedUriArgs)
        throws URISyntaxException {
        uriBuilder = new URIBuilder(IPv4_URL_NO_PORT);

        testUriBuilder(arguments, uriBuilder, IPv4_URL_NO_PORT, expectedUriArgs);
    }

    @ParameterizedTest
    @MethodSource("uriArgs")
    public void shouldBuildUriForHostNoSchema(List<LookupArg> arguments, String expectedUriArgs)
        throws URISyntaxException {
        uriBuilder = new URIBuilder(HOST_URL_NO_SCHEMA);

        testUriBuilder(arguments, uriBuilder, HOST_URL_NO_SCHEMA, expectedUriArgs);
    }

    @ParameterizedTest
    @MethodSource("uriArgs")
    public void shouldBuildUriForIPv4NoSchema(List<LookupArg> arguments, String expectedUriArgs)
        throws URISyntaxException {
        uriBuilder = new URIBuilder(IPv4_URL_NO_SCHEMA);

        testUriBuilder(arguments, uriBuilder, IPv4_URL_NO_SCHEMA, expectedUriArgs);
    }

    @ParameterizedTest
    @MethodSource("uriArgs")
    public void shouldBuildUriForHostUserInfo(List<LookupArg> arguments, String expectedUriArgs)
        throws URISyntaxException {
        uriBuilder = new URIBuilder(HOST_URL_USER_INFO);

        testUriBuilder(arguments, uriBuilder, HOST_URL_USER_INFO, expectedUriArgs);
    }

    @ParameterizedTest
    @MethodSource("uriArgs")
    public void shouldBuildUriForIPv4UserInfo(List<LookupArg> arguments, String expectedUriArgs)
        throws URISyntaxException {
        uriBuilder = new URIBuilder(IPv4_URL_USER_INFO);

        testUriBuilder(arguments, uriBuilder, IPv4_URL_USER_INFO, expectedUriArgs);
    }

    @ParameterizedTest
    @MethodSource("uriArgs")
    public void shouldBuildUriForHostNoPath(List<LookupArg> arguments, String expectedUriArgs)
        throws URISyntaxException {
        uriBuilder = new URIBuilder(HOST_URL_NO_PATH);

        testUriBuilder(arguments, uriBuilder, HOST_URL_NO_PATH, expectedUriArgs);
    }

    @ParameterizedTest
    @MethodSource("uriArgs")
    public void shouldBuildUriForIPv4NoPath(List<LookupArg> arguments, String expectedUriArgs)
        throws URISyntaxException {
        uriBuilder = new URIBuilder(IPv4_URL_NO_PATH);

        testUriBuilder(arguments, uriBuilder, IPv4_URL_NO_PATH, expectedUriArgs);
    }

    @ParameterizedTest
    @MethodSource("uriArgs")
    public void shouldBuildUriForHostWithParams(List<LookupArg> arguments, String expectedUriArgs)
        throws URISyntaxException {
        uriBuilder = new URIBuilder(HOST_URL_WITH_PARAMS);

        testUriBuilder(
            arguments,
            uriBuilder,
            HOST_URL_WITH_PARAMS,
            expectedUriArgs.replaceFirst("\\?", "&")
        );
    }

    @ParameterizedTest
    @MethodSource("uriArgs")
    public void shouldBuildUriForHostParamsMark(List<LookupArg> arguments, String expectedUriArgs)
        throws URISyntaxException {
        uriBuilder = new URIBuilder(HOST_URL_WITH_PARAMS_MARK);

        testUriBuilder(
            arguments,
            uriBuilder,
            HOST_URL_WITH_PARAMS_MARK,
            expectedUriArgs.replaceFirst("\\?", "")
        );
    }

    @ParameterizedTest
    @MethodSource("uriArgs")
    public void shouldBuildUriForHostUrlEnd(List<LookupArg> arguments, String expectedUriArgs)
        throws URISyntaxException {
        uriBuilder = new URIBuilder(HOST_URL_WITH_END);

        testUriBuilder(arguments, uriBuilder, HOST_URL_WITH_END, expectedUriArgs);
    }

    @Test
    public void testHierarchicalUri() throws Exception {
        final URI uri =
            new URI("http", "stuff", "localhost", 80, "/some stuff", "param=stuff", "fragment");
        final URIBuilder uribuilder = new URIBuilder(uri);
        final URI result = uribuilder.build();
        assertThat(result).isEqualTo(
            new URI("http://stuff@localhost:80/some%20stuff?param=stuff#fragment"));
    }

    @Test
    public void testOpaqueUri() throws Exception {
        final URI uri = new URI("stuff", "some-stuff", "fragment");
        final URIBuilder uribuilder = new URIBuilder(uri);
        final URI result = uribuilder.build();
        assertThat(result).isEqualTo(uri);
    }

    @Test
    public void testParameterWithSpecialChar() throws Exception {
        final URI uri = new URI("http", null, "localhost", 80, "/", "param=stuff", null);
        final URIBuilder uribuilder = new URIBuilder(uri).addParameter("param", "1 + 1 = 2")
            .addParameter("param", "blah&blah");
        final URI result = uribuilder.build();
        assertThat(result)
            .isEqualTo(new URI("http://localhost:80/?param=stuff&param=1+%2B+1+%3D+2&" +
                "param=blah%26blah")
            );
    }

    @Test
    public void testAddParameter() throws Exception {
        final URI uri = new URI("http", null, "localhost", 80, "/", "param=stuff&blah&blah", null);
        final URIBuilder uribuilder = new URIBuilder(uri).addParameter("param", "some other stuff")
            .addParameter("blah", "blah");
        final URI result = uribuilder.build();
        assertThat(result)
            .isEqualTo(new URI("http://localhost:80/?param=stuff&blah&blah&" +
                "param=some+other+stuff&blah=blah")
            );
    }

    @Test
    public void testQueryEncoding() throws Exception {
        final URI uri1 = new URI("https://somehost.com/stuff?client_id=1234567890" +
            "&redirect_uri=https%3A%2F%2Fsomehost.com%2Fblah+blah%2F");
        final URI uri2 = new URIBuilder("https://somehost.com/stuff")
            .addParameter("client_id", "1234567890")
            .addParameter("redirect_uri", "https://somehost.com/blah blah/").build();
        assertThat(uri2).isEqualTo(uri1);
    }

    @Test
    public void testRelativePath() throws Exception {
        final URI uri = new URIBuilder("./mypath").build();
        assertThat(uri).isEqualTo(new URI("./mypath"));
    }

    private void testUriBuilder(
            List<LookupArg> arguments,
            URIBuilder uriBuilder,
            String baseUrl,
            String expectedUriArgs) throws URISyntaxException {

        for (LookupArg arg : arguments) {
            uriBuilder.addParameter(arg.getArgName(), arg.getArgValue());
        }

        URI uri = uriBuilder.build();
        System.out.println(uri);
        assertThat(uri.toString()).isEqualTo(baseUrl + expectedUriArgs);
    }
}

