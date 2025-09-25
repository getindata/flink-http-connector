package com.getindata.connectors.http.internal.table.lookup;

import java.util.Collections;
import java.util.HashMap;

import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;

public class HttpRowDataWrapperTest {

    @Test
    void testshouldIgnore() {
        HttpRowDataWrapper httpRowDataWrapper = new HttpRowDataWrapper(Collections.emptyList(),
                null,
                null,
                null,
                HttpCompletionState.SUCCESS);
        assertThat(httpRowDataWrapper.shouldIgnore()).isTrue();
        httpRowDataWrapper = new HttpRowDataWrapper(null,
                "aa",
                null,
                null,
                HttpCompletionState.SUCCESS);
        assertThat(httpRowDataWrapper.shouldIgnore()).isFalse();
        httpRowDataWrapper = new HttpRowDataWrapper(Collections.emptyList(),
                "aa",
                null,
                null,
                HttpCompletionState.SUCCESS);
        assertThat(httpRowDataWrapper.shouldIgnore()).isFalse();
        httpRowDataWrapper = new HttpRowDataWrapper(Collections.emptyList(),
                null,
                new HashMap<>(),
                null,
                HttpCompletionState.SUCCESS);
        assertThat(httpRowDataWrapper.shouldIgnore()).isFalse();
        httpRowDataWrapper = new HttpRowDataWrapper(Collections.emptyList(),
                null,
                null,
                123,
                HttpCompletionState.SUCCESS);
        assertThat(httpRowDataWrapper.shouldIgnore()).isFalse();
        httpRowDataWrapper = new HttpRowDataWrapper(Collections.emptyList(),
                null,
                null,
                null,
                HttpCompletionState.EXCEPTION);
        assertThat(httpRowDataWrapper.shouldIgnore()).isFalse();
    }
}
