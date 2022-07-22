package com.getindata.connectors.http.internal.sink.httpclient.status;

import java.util.HashMap;
import java.util.Map;

/**
 * This enum represents HTTP response code types, grouped by "hundreds" digit.
 */
public enum HttpType {

    INFO(1),
    SUCCESS(2),
    REDIRECTION(3),
    CLIENT_ERROR(4),
    SERVER_ERROR(5);

    private static final Map<Integer, HttpType> map;

    static {
        map = new HashMap<>();
        for (HttpType httpType : HttpType.values()) {
            map.put(httpType.httpTypeCode, httpType);
        }
    }

    private final int httpTypeCode;

    HttpType(int httpTypeCode) {
        this.httpTypeCode = httpTypeCode;
    }

    /**
     * @return a {@link HttpType} instance based on http type code,
     * for example {@code HttpType.getByCode(1)} will return {@link HttpType#INFO} type.
     */
    public static HttpType getByCode(int i) {
        return map.get(i);
    }

    /**
     * @return a "hundrets" digit that represents given {@link HttpType} instance. For example
     * {@code HttpType.INFO.getHttpTypeCode()} will return 1 since HTTP information repossess have
     * status codes in range 100 - 199.
     */
    public int getHttpTypeCode() {
        return this.httpTypeCode;
    }
}
