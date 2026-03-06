package com.getindata.connectors.http.internal.config;

public enum ResponseItemStatus {
    SUCCESS("success"),
    TEMPORAL("temporal"),
    IGNORE("ignore"),
    FAILURE("failure");

    private final String status;

    ResponseItemStatus(String status) {
        this.status = status;
    }

    public String getStatus() {
        return status;
    }
}
