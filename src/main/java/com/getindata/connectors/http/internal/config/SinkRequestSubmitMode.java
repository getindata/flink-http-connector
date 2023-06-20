package com.getindata.connectors.http.internal.config;

public enum SinkRequestSubmitMode {

    PER_REQUEST("PerRequest"),
    BATCH("Batch");

    private final String mode;

    SinkRequestSubmitMode(String mode) {
        this.mode = mode;
    }

    public String getMode() {
        return mode;
    }
}
