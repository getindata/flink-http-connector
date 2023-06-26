package com.getindata.connectors.http.internal.config;

public enum SinkRequestSubmitMode {

    SINGLE("single"),
    BATCH("batch");

    private final String mode;

    SinkRequestSubmitMode(String mode) {
        this.mode = mode;
    }

    public String getMode() {
        return mode;
    }
}
