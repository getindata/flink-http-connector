package com.getindata.connectors.http.internal;

public enum HttpLoggingLevelType {
    MIN,
    REQRESPONSE,
    MAX;

    public static HttpLoggingLevelType valueOfStr(String code) {
        if (code == null) {
            return MIN;
        } else {
            return valueOf(code);
        }
    }
}
