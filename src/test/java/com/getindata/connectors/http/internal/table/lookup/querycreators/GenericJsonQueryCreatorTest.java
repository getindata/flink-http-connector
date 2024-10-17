package com.getindata.connectors.http.internal.table.lookup.querycreators;

import java.util.List;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.formats.json.JsonFormatFactory;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.factories.DynamicTableFactory.Context;
import org.apache.flink.table.types.DataType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import static org.assertj.core.api.Assertions.assertThat;

import com.getindata.connectors.http.internal.table.lookup.LookupQueryInfo;
import static com.getindata.connectors.http.internal.table.lookup.HttpLookupTableSourceFactory.row;

@ExtendWith(MockitoExtension.class)
class GenericJsonQueryCreatorTest {

    @Mock
    private Context dynamicTableFactoryContext;

    private GenericJsonQueryCreator jsonQueryCreator;

    @BeforeEach
    public void setUp() {

        DataType lookupPhysicalDataType = row(List.of(
                DataTypes.FIELD("id", DataTypes.INT()),
                DataTypes.FIELD("uuid", DataTypes.STRING())
            )
        );

        SerializationSchema<RowData> jsonSerializer =
            new JsonFormatFactory()
                .createEncodingFormat(dynamicTableFactoryContext, new Configuration())
                .createRuntimeEncoder(null, lookupPhysicalDataType);

        this.jsonQueryCreator = new GenericJsonQueryCreator(jsonSerializer);
    }

    @Test
    public void shouldSerializeToJson() {
        GenericRowData row = new GenericRowData(2);
        row.setField(0, 11);
        row.setField(1, StringData.fromString("myUuid"));

        LookupQueryInfo lookupQuery = this.jsonQueryCreator.createLookupQuery(row);
        assertThat(lookupQuery.getBodyBasedUrlQueryParameters().isEmpty());
        assertThat(lookupQuery.getLookupQuery()).isEqualTo("{\"id\":11,\"uuid\":\"myUuid\"}");
    }

    @Test
    public void shouldSerializeToJsonTwice() {
        GenericRowData row = new GenericRowData(2);
        row.setField(0, 11);
        row.setField(1, StringData.fromString("myUuid"));

        // Call createLookupQuery two times
        // to check that serialization schema is not opened Two times.
        this.jsonQueryCreator.createLookupQuery(row);
        LookupQueryInfo lookupQuery = this.jsonQueryCreator.createLookupQuery(row);
        assertThat(lookupQuery.getBodyBasedUrlQueryParameters().isEmpty());
        assertThat(lookupQuery.getLookupQuery()).isEqualTo("{\"id\":11,\"uuid\":\"myUuid\"}");
    }
}
