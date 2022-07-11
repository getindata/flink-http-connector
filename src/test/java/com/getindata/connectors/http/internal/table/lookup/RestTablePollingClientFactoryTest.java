package com.getindata.connectors.http.internal.table.lookup;

import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.table.functions.FunctionContext;
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
    void shouldCreateClient() {

        assertThat(factory.createPollClient(mock(SourceReaderContext.class)))
            .isInstanceOf(RestTablePollingClient.class);

        assertThat(
            factory.createPollClient(mock(FunctionContext.class), mock(HttpLookupConfig.class)))
            .isInstanceOf(RestTablePollingClient.class);
    }
}
