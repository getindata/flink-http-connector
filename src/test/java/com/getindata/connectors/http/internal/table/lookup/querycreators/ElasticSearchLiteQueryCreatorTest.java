package com.getindata.connectors.http.internal.table.lookup.querycreators;

import java.math.BigDecimal;
import java.util.List;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.DataType;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;

import com.getindata.connectors.http.internal.table.lookup.LookupRow;
import com.getindata.connectors.http.internal.table.lookup.RowDataSingleValueLookupSchemaEntry;
import static com.getindata.connectors.http.internal.table.lookup.HttpLookupTableSourceFactory.row;

public class ElasticSearchLiteQueryCreatorTest {

    @Test
    public void testQueryCreationForSingleQueryStringParam() {

        // GIVEN
        LookupRow lookupRow = new LookupRow()
            .addLookupEntry(
                new RowDataSingleValueLookupSchemaEntry("key1",
                    RowData.createFieldGetter(DataTypes.STRING().getLogicalType(), 0))
            );
        lookupRow.setLookupPhysicalRowDataType(DataTypes.STRING());

        GenericRowData lookupDataRow = GenericRowData.of(StringData.fromString("val1"));

        // WHEN
        var queryCreator = new ElasticSearchLiteQueryCreator(lookupRow);
        var createdQuery = queryCreator.createLookupQuery(lookupDataRow);

        // THEN
        assertThat(createdQuery.getLookupQuery()).isEqualTo("q=key1:%22val1%22");
        assertThat(createdQuery.getBodyBasedUrlQueryParameters()).isEmpty();
    }

    @Test
    public void testQueryCreationForSingleQueryIntParam() {

        // GIVEN
        BigDecimal decimalValue = BigDecimal.valueOf(10);
        DataType decimalValueType = DataTypes.DECIMAL(
            decimalValue.precision(),
            decimalValue.scale()
        );

        LookupRow lookupRow = new LookupRow()
            .addLookupEntry(
                new RowDataSingleValueLookupSchemaEntry("key1",
                    RowData.createFieldGetter(
                        decimalValueType.getLogicalType(),
                        0)
                )
            );
        lookupRow.setLookupPhysicalRowDataType(decimalValueType);

        GenericRowData lookupDataRow = GenericRowData.of(
            DecimalData.fromBigDecimal(decimalValue, decimalValue.precision(),
                decimalValue.scale()));

        // WHEN
        var queryCreator = new ElasticSearchLiteQueryCreator(lookupRow);
        var createdQuery = queryCreator.createLookupQuery(lookupDataRow);

        // THEN
        assertThat(createdQuery.getLookupQuery()).isEqualTo("q=key1:%2210%22");
        assertThat(createdQuery.getBodyBasedUrlQueryParameters()).isEmpty();
    }

    @Test
    public void testGenericGetQueryCreationForMultipleQueryParam() {

        // GIVEN
        LookupRow lookupRow = new LookupRow()
            .addLookupEntry(
                new RowDataSingleValueLookupSchemaEntry(
                    "key1",
                    RowData.createFieldGetter(DataTypes.STRING().getLogicalType(), 0)
                ))
            .addLookupEntry(
                new RowDataSingleValueLookupSchemaEntry(
                    "key2",
                    RowData.createFieldGetter(DataTypes.STRING().getLogicalType(), 1)
                ))
            .addLookupEntry(
                new RowDataSingleValueLookupSchemaEntry(
                    "key3",
                    RowData.createFieldGetter(DataTypes.STRING().getLogicalType(), 2)
                ));

        lookupRow.setLookupPhysicalRowDataType(
            row(List.of(
                DataTypes.FIELD("key1", DataTypes.STRING()),
                DataTypes.FIELD("key2", DataTypes.STRING()),
                DataTypes.FIELD("key3", DataTypes.STRING())
            )));

        GenericRowData lookupDataRow = GenericRowData.of(
            StringData.fromString("val1"),
            StringData.fromString("val2"),
            StringData.fromString("3")
        );

        // WHEN
        var queryCreator = new ElasticSearchLiteQueryCreator(lookupRow);
        var createdQuery = queryCreator.createLookupQuery(lookupDataRow);

        // THEN
        assertThat(createdQuery.getLookupQuery())
                .isEqualTo("q=key1:%22val1%22%20AND%20key2:%22val2%22%20AND%20key3:%223%22");
        assertThat(createdQuery.getBodyBasedUrlQueryParameters())
                .isEmpty();
    }
}
