package com.getindata.connectors.http;

import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import com.github.tomakehurst.wiremock.WireMockServer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.getindata.connectors.http.internal.sink.HttpSinkRequestEntry;
import com.getindata.connectors.http.internal.table.sink.HttpDynamicTableSinkFactory;
import static com.getindata.connectors.http.TestPostRequestCallbackFactory.TEST_POST_REQUEST_CALLBACK_IDENT;

public class HttpPostRequestCallbackFactoryTest {
    private static final int SERVER_PORT = 9090;

    private WireMockServer wireMockServer;
    protected StreamExecutionEnvironment env;
    protected StreamTableEnvironment tEnv;

    private static final ArrayList<HttpSinkRequestEntry> requestEntries = new ArrayList<>();
    private static final ArrayList<HttpResponse<String>> responses = new ArrayList<>();

    @BeforeEach
    public void setup() {
        wireMockServer = new WireMockServer(SERVER_PORT);
        wireMockServer.start();

        env = StreamExecutionEnvironment.getExecutionEnvironment();
        tEnv = StreamTableEnvironment.create(env);

        requestEntries.clear();
        responses.clear();
    }

    @AfterEach
    public void tearDown() {
        wireMockServer.stop();
    }

    @Test
    public void httpPostRequestCallbackFactoryTest()
        throws ExecutionException, InterruptedException {
        wireMockServer.stubFor(any(urlPathEqualTo("/myendpoint")).willReturn(ok()));

        final String createTable =
            String.format(
                "CREATE TABLE http (\n"
                + "  id bigint\n"
                + ") with (\n"
                + "  'connector' = '%s',\n"
                + "  'url' = '%s',\n"
                + "  'format' = 'json',\n"
                + "  'gid.connector.http.sink.request-callback' = '%s',\n"
                + "  'gid.connector.http.sink.header.Content-Type' = 'application/json'\n"
                + ")",
                HttpDynamicTableSinkFactory.IDENTIFIER,
                "http://localhost:" + SERVER_PORT + "/myendpoint",
                TEST_POST_REQUEST_CALLBACK_IDENT
            );
        tEnv.executeSql(createTable);

        final String insert = "INSERT INTO http VALUES (1)";
        tEnv.executeSql(insert).await();

        assertEquals(1, requestEntries.size());
        assertEquals(1, responses.size());

        assertArrayEquals(
            "{\"id\":1}".getBytes(StandardCharsets.UTF_8),
            requestEntries.get(0).element
        );
    }

    public static class TestPostRequestCallback
        implements HttpPostRequestCallback<HttpSinkRequestEntry> {
        @Override
        public void call(
            HttpResponse<String> response,
            HttpSinkRequestEntry requestEntry,
            String endpointUrl,
            Map<String, String> headerMap
        ) {
            requestEntries.add(requestEntry);
            responses.add(response);
        }
    }
}
