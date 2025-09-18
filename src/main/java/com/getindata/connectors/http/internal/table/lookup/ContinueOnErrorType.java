package com.getindata.connectors.http.internal.table.lookup;

public enum ContinueOnErrorType {
    HTTP_FAILED_AFTER_RETRY,
    HTTP_FAILED,
    CLIENT_SIDE_EXCEPTION,
    NONE
}
