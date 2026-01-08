package com.getindata.connectors.http.internal.table.lookup;

public enum HttpCompletionState {
    HTTP_ERROR_STATUS,
    EXCEPTION,
    SUCCESS,
    UNABLE_TO_DESERIALIZE_RESPONSE,
    IGNORE_STATUS_CODE
}
