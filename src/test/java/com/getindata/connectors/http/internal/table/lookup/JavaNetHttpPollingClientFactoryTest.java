package com.getindata.connectors.http.internal.table.lookup;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.table.data.RowData;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

class JavaNetHttpPollingClientFactoryTest {

    private JavaNetHttpPollingClientFactory factory;

    @BeforeEach
    public void setUp() {
        factory = new JavaNetHttpPollingClientFactory();
    }

    @Test
    @SuppressWarnings("unchecked")
    void shouldCreateClient() {

        assertThat(
            factory.createPollClient(
                HttpLookupConfig.builder().build(),
                (DeserializationSchema<RowData>) mock(DeserializationSchema.class))
        ).isInstanceOf(JavaNetHttpPollingClient.class);
    }
}
