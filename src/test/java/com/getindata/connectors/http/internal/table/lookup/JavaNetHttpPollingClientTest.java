package com.getindata.connectors.http.internal.table.lookup;

import java.net.http.HttpClient;
import java.util.Properties;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.table.data.RowData;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import static org.assertj.core.api.Assertions.assertThat;

import com.getindata.connectors.http.internal.HeaderPreprocessor;
import com.getindata.connectors.http.internal.config.HttpConnectorConfigConstants;
import com.getindata.connectors.http.internal.table.lookup.querycreators.GenericGetQueryCreator;
import com.getindata.connectors.http.internal.utils.HttpHeaderUtils;
import static com.getindata.connectors.http.TestHelper.assertPropertyArray;

@ExtendWith(MockitoExtension.class)
public class JavaNetHttpPollingClientTest {

    @Mock
    private HttpClient httpClient;

    @Mock
    private DeserializationSchema<RowData> decoder;

    @Mock
    private LookupRow lookupRow;

    private HeaderPreprocessor headerPreprocessor;

    private HttpLookupConfig options;

    @BeforeEach
    public void setUp() {
        this.headerPreprocessor = HttpHeaderUtils.createDefaultHeaderPreprocessor();
        this.options = HttpLookupConfig.builder().build();
    }

    @Test
    public void shouldBuildClientWithoutHeaders() {

        JavaNetHttpPollingClient client = new JavaNetHttpPollingClient(
            httpClient,
            decoder,
            options,
            new GetRequestFactory(
                new GenericGetQueryCreator(lookupRow),
                headerPreprocessor,
                options
            )
        );

        assertThat(
            ((GetRequestFactory) client.getRequestFactory()).getHeadersAndValues())
            .isEmpty();
    }

    @Test
    public void shouldBuildClientWithHeaders() {

        // GIVEN
        Properties properties = new Properties();
        properties.setProperty("property", "val1");
        properties.setProperty("my.property", "val2");
        properties.setProperty(
            HttpConnectorConfigConstants.LOOKUP_SOURCE_HEADER_PREFIX + "Origin",
            "https://developer.mozilla.org");

        properties.setProperty(
            HttpConnectorConfigConstants.LOOKUP_SOURCE_HEADER_PREFIX + "Cache-Control",
            "no-cache, no-store, max-age=0, must-revalidate"
        );
        properties.setProperty(
            HttpConnectorConfigConstants.LOOKUP_SOURCE_HEADER_PREFIX
                + "Access-Control-Allow-Origin", "*"
        );

        // WHEN
        HttpLookupConfig lookupConfig = HttpLookupConfig.builder()
            .properties(properties)
            .build();

        JavaNetHttpPollingClient client = new JavaNetHttpPollingClient(
            httpClient,
            decoder,
            lookupConfig,
            new GetRequestFactory(
                new GenericGetQueryCreator(lookupRow),
                headerPreprocessor,
                lookupConfig
            )
        );

        String[] headersAndValues =
            ((GetRequestFactory) client.getRequestFactory()).getHeadersAndValues();
        assertThat(headersAndValues).hasSize(6);

        // THEN
        // assert that we have property followed by its value.
        assertPropertyArray(headersAndValues, "Origin", "https://developer.mozilla.org");
        assertPropertyArray(
            headersAndValues,
            "Cache-Control", "no-cache, no-store, max-age=0, must-revalidate"
        );
        assertPropertyArray(headersAndValues, "Access-Control-Allow-Origin", "*");
    }
}
