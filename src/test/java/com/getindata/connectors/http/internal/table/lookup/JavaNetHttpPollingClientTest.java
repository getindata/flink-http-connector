package com.getindata.connectors.http.internal.table.lookup;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.util.*;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.formats.json.JsonFormatFactory;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.types.DataType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import static org.assertj.core.api.Assertions.assertThat;


import com.getindata.connectors.http.internal.HeaderPreprocessor;
import com.getindata.connectors.http.internal.config.HttpConnectorConfigConstants;
import com.getindata.connectors.http.internal.table.lookup.querycreators.GenericGetQueryCreator;
import com.getindata.connectors.http.internal.table.lookup.querycreators.GenericJsonQueryCreator;
import com.getindata.connectors.http.internal.utils.HttpHeaderUtils;
import static com.getindata.connectors.http.TestHelper.assertPropertyArray;
import static com.getindata.connectors.http.internal.table.lookup.HttpLookupTableSourceFactory.row;

@ExtendWith(MockitoExtension.class)
public class JavaNetHttpPollingClientTest {

    @Mock
    private HttpClient httpClient;

    @Mock
    private DeserializationSchema<RowData> decoder;

    @Mock
    private LookupRow lookupRow;

    @Mock
    private DynamicTableFactory.Context dynamicTableFactoryContext;
    private HeaderPreprocessor headerPreprocessor;

    private HttpLookupConfig options;

    private static final String BASE_URL = "http://localhost.com";

    @BeforeEach
    public void setUp() {
        this.headerPreprocessor = HttpHeaderUtils.createDefaultHeaderPreprocessor();
        this.options = HttpLookupConfig.builder().url(BASE_URL).build();
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
    public void shouldBuildGetClientUri() {
        // GIVEN
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

        DataType lookupPhysicalDataType = row(List.of(
                        DataTypes.FIELD("id", DataTypes.STRING()),
                        DataTypes.FIELD("uuid", DataTypes.STRING())
                ));

        RowData lookupRowData = GenericRowData.of(
                StringData.fromString("1"),
                StringData.fromString("2")
        );

        LookupRow lookupRow = new LookupRow()
                .addLookupEntry(
                        new RowDataSingleValueLookupSchemaEntry("id",
                                RowData.createFieldGetter(
                                        DataTypes.STRING().getLogicalType(),
                                        0)))
                .addLookupEntry(
                        new RowDataSingleValueLookupSchemaEntry("uuid",
                                RowData.createFieldGetter(
                                        DataTypes.STRING().getLogicalType(),
                                        1))
                );
        lookupRow.setLookupPhysicalRowDataType(lookupPhysicalDataType);

        GenericGetQueryCreator queryCreator = new GenericGetQueryCreator(lookupRow);
        LookupQueryInfo lookupQueryInfo = queryCreator.createLookupQuery(lookupRowData);

        // WHEN
        URI uri = ((GetRequestFactory) client.getRequestFactory()).constructGetUri(lookupQueryInfo);

        // THEN
        assertThat(uri.toString()).isEqualTo(BASE_URL + "?id=1&uuid=2");
    }

    @Test
    public void shouldBuildBodyBasedClientUri() {
        // GIVEN
        DataType lookupPhysicalDataType = row(List.of(
                DataTypes.FIELD("id", DataTypes.STRING()),
                DataTypes.FIELD("uuid", DataTypes.STRING())
        ));

        LookupRow lookupRow = new LookupRow()
                .addLookupEntry(
                        new RowDataSingleValueLookupSchemaEntry("id",
                                RowData.createFieldGetter(
                                        DataTypes.STRING().getLogicalType(),
                                        0)))
                .addLookupEntry(
                        new RowDataSingleValueLookupSchemaEntry("uuid",
                                RowData.createFieldGetter(
                                        DataTypes.STRING().getLogicalType(),
                                        1))
                );

        lookupRow.setLookupPhysicalRowDataType(lookupPhysicalDataType);

        SerializationSchema<RowData> jsonSerializer =
                new JsonFormatFactory()
                        .createEncodingFormat(dynamicTableFactoryContext, new Configuration())
                        .createRuntimeEncoder(null, lookupPhysicalDataType);

        BodyBasedRequestFactory requestFactory = new BodyBasedRequestFactory(
                "POST",
                new GenericJsonQueryCreator(jsonSerializer),
                HttpHeaderUtils.createDefaultHeaderPreprocessor(),
                HttpLookupConfig.builder()
                        .url(BASE_URL)
                        .build()
        );

        Map<String, String> urlBodyBasedQueryParameters = new LinkedHashMap<>();
        urlBodyBasedQueryParameters.put("key1", "value1");
        urlBodyBasedQueryParameters.put("key2", "value2");

        LookupQueryInfo lookupQueryInfo = new LookupQueryInfo("{}", urlBodyBasedQueryParameters);

        // WHEN
        HttpRequest httpRequest = requestFactory.setUpRequestMethod(lookupQueryInfo).build();

        // THEN
        assertThat(httpRequest.uri().toString()).isEqualTo(BASE_URL + "?key1=value1&key2=value2");
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
