package com.getindata.connectors.http.internal.table.lookup;

import java.util.Collections;
import java.util.HashMap;

import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;

public class HttpRowDataWrapperTest {

    @Test
    void testshouldIgnore() {
        HttpRowDataWrapper httpRowDataWrapper = HttpRowDataWrapper.builder()
                .data(Collections.emptyList())
                .httpCompletionState(HttpCompletionState.SUCCESS)
                .build();
        assertThat(httpRowDataWrapper.shouldIgnore()).isTrue();
        httpRowDataWrapper = HttpRowDataWrapper.builder()
                .errorMessage("aa")
                .httpCompletionState(HttpCompletionState.SUCCESS)
                .build();
        assertThat(httpRowDataWrapper.shouldIgnore()).isFalse();
        httpRowDataWrapper = HttpRowDataWrapper.builder()
                .data(Collections.emptyList())
                .errorMessage("aa")
                .httpCompletionState(HttpCompletionState.SUCCESS)
                .build();
        assertThat(httpRowDataWrapper.shouldIgnore()).isFalse();
        httpRowDataWrapper = HttpRowDataWrapper.builder()
                .data(Collections.emptyList())
                .httpHeadersMap(new HashMap<>())
                .httpCompletionState(HttpCompletionState.SUCCESS)
                .build();
        assertThat(httpRowDataWrapper.shouldIgnore()).isFalse();
        httpRowDataWrapper = HttpRowDataWrapper.builder()
                .data(Collections.emptyList())
                .httpStatusCode(123)
                .httpCompletionState(HttpCompletionState.SUCCESS)
                .build();
        assertThat(httpRowDataWrapper.shouldIgnore()).isFalse();
        httpRowDataWrapper = HttpRowDataWrapper.builder()
                .data(Collections.emptyList())
                .httpCompletionState(HttpCompletionState.EXCEPTION)
                .build();
        assertThat(httpRowDataWrapper.shouldIgnore()).isFalse();
    }
}
