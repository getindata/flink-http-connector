package com.getindata.connectors.http.internal;

import java.net.http.HttpHeaders;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.StringJoiner;

import lombok.extern.slf4j.Slf4j;

import com.getindata.connectors.http.internal.table.lookup.HttpLookupSourceRequestEntry;
import static com.getindata.connectors.http.internal.config.HttpConnectorConfigConstants.HTTP_LOGGING_LEVEL;

@Slf4j
public class HttpLogger {

    private final HttpLoggingLevelType httpLoggingLevelType;

    public static HttpLogger getHttpLogger(Properties properties) {
        return new HttpLogger(properties);
    }

    public void logRequest(HttpRequest httpRequest) {
        log.debug(createStringForRequest(httpRequest));
    }

    public void logResponse(HttpResponse<String> response) {
        log.debug(createStringForResponse(response));
    }

    public void logRequestBody(String body) {
        log.debug(createStringForBody(body));
    }

    public void logExceptionResponse(HttpLookupSourceRequestEntry request, Exception e) {
        log.debug(createStringForExceptionResponse(request, e));
    }

    private HttpLogger(Properties properties) {
        String code = (String) properties.get(HTTP_LOGGING_LEVEL);
        this.httpLoggingLevelType = HttpLoggingLevelType.valueOfStr(code);
    }

    String createStringForRequest(HttpRequest httpRequest) {
        String headersForLog = getHeadersForLog(httpRequest.headers());
        return String.format("HTTP %s Request: URL: %s, Headers: %s",
            httpRequest.method(),
            httpRequest.uri().toString(),
            headersForLog
        );
    }

    private String getHeadersForLog(HttpHeaders httpHeaders) {
        if (httpHeaders == null) return "None";
        Map<String, List<String>> headersMap = httpHeaders.map();
        if (headersMap.isEmpty()) return "None";
        if (this.httpLoggingLevelType == HttpLoggingLevelType.MAX) {
            StringJoiner headers = new StringJoiner(";");
            for (Map.Entry<String, List<String>> reqHeaders : headersMap.entrySet()) {
                StringJoiner values = new StringJoiner(";");
                for (String value : reqHeaders.getValue()) {
                    values.add(value);
                }
                String header = reqHeaders.getKey() + ":[" + values + "]";
                headers.add(header);
            }
            return headers.toString();
        }
        return "***";
    }

    String createStringForResponse(HttpResponse<String> response) {
        String headersForLog = getHeadersForLog(response.headers());

        String bodyForLog = "***";
        if (response.body() == null || response.body().isEmpty()) {
            bodyForLog = "None";
        } else {
            if (this.httpLoggingLevelType != HttpLoggingLevelType.MIN) {
                bodyForLog = response.body().toString();
            }
        }
        return String.format("HTTP %s Response: URL: %s,"
                + " Response Headers: %s, status code: %s, Response Body: %s",
            response.request().method(),
            response.uri(),
            headersForLog,
            response.statusCode(),
            bodyForLog
        );
    }

    private String createStringForExceptionResponse(HttpLookupSourceRequestEntry request, Exception e) {
        HttpRequest httpRequest = request.getHttpRequest();
        return String.format("HTTP %s Exception Response: URL: %s Exception %s",
            httpRequest.method(),
            httpRequest.uri(),
            e
        );
    }

    String createStringForBody(String body) {
        String bodyForLog = "***";
        if (body == null || body.isEmpty()) {
            bodyForLog = "None";
        } else {
            if (this.httpLoggingLevelType != HttpLoggingLevelType.MIN) {
                bodyForLog = body.toString();
            }
        }
        return bodyForLog;
    }
}
