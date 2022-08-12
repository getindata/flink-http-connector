package com.getindata.connectors.http.internal.table.lookup;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.table.data.RowData;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

class RestTablePollingClientFactoryTest {

    private RestTablePollingClientFactory factory;

    @BeforeEach
    public void setUp() {
        factory = new RestTablePollingClientFactory();
    }

    @Test
    @SuppressWarnings("unchecked")
    void shouldCreateClient() {
        assertThat(
            factory.createPollClient(
                mock(HttpLookupConfig.class),
                (DeserializationSchema<RowData>) mock(DeserializationSchema.class))
        ).isInstanceOf(RestTablePollingClient.class);
    }
}
