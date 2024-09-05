package com.getindata.connectors.http.internal.table.lookup;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;

class JavaNetHttpPollingClientFactoryTest {

    private JavaNetHttpPollingClientFactory factory;

    @BeforeEach
    public void setUp() {
        factory = new JavaNetHttpPollingClientFactory();
    }

    @Test
    void shouldCreateClient() {

        assertThat(
            factory.createPollClient(
                HttpLookupConfig.builder().build())
        ).isInstanceOf(JavaNetHttpPollingClient.class);
    }
}
