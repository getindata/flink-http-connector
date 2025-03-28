package com.getindata.connectors.http.internal.table.lookup;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.ConfigurationException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

class JavaNetHttpPollingClientFactoryTest {

    private JavaNetHttpPollingClientFactory factory;

    @BeforeEach
    public void setUp() {
        factory = new JavaNetHttpPollingClientFactory(mock(GetRequestFactory.class));
    }

    @Test
    @SuppressWarnings("unchecked")
    void shouldCreateClient() throws ConfigurationException {

        assertThat(
            factory.createPollClient(
                HttpLookupConfig.builder().build(),
                (DeserializationSchema<RowData>) mock(DeserializationSchema.class))
        ).isInstanceOf(JavaNetHttpPollingClient.class);
    }
}
