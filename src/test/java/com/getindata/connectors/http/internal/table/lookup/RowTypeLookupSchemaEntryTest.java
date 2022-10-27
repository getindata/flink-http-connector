package com.getindata.connectors.http.internal.table.lookup;

import java.util.List;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;

import com.getindata.connectors.http.LookupArg;

class RowTypeLookupSchemaEntryTest {

    @Test
    public void testEmptyRow() {
        // GIVEN
        RowTypeLookupSchemaEntry lookupSchemaEntry = new RowTypeLookupSchemaEntry(
            "aRow",
            RowData.createFieldGetter(
                DataTypes.ROW(
                        DataTypes.FIELD("col1", DataTypes.STRING()))
                    .getLogicalType(),
                0))
            .addLookupEntry(new RowDataSingleValueLookupSchemaEntry(
                "col1",
                RowData.createFieldGetter(DataTypes.STRING().getLogicalType(), 0))
            );

        GenericRowData rowData = new GenericRowData(1);
        rowData.setField(0, null);

        // WHEN
        List<LookupArg> lookupArgs = lookupSchemaEntry.convertToLookupArg(rowData);

        // THEN
        assertThat(lookupArgs).isEmpty();
    }

    @Test
    public void testRowWithMultipleSingleValues() {

        // GIVEN
        RowTypeLookupSchemaEntry lookupSchemaEntry = new RowTypeLookupSchemaEntry(
            "aRow",
            RowData.createFieldGetter(
                DataTypes.ROW(
                        DataTypes.FIELD("col1", DataTypes.STRING()),
                        DataTypes.FIELD("col2", DataTypes.STRING()),
                        DataTypes.FIELD("col3", DataTypes.STRING()))
                    .getLogicalType(),
                0))
            .addLookupEntry(new RowDataSingleValueLookupSchemaEntry(
                "col1",
                RowData.createFieldGetter(DataTypes.STRING().getLogicalType(), 0))
            )
            .addLookupEntry(new RowDataSingleValueLookupSchemaEntry(
                "col2",
                RowData.createFieldGetter(DataTypes.STRING().getLogicalType(), 1))
            )
            .addLookupEntry(new RowDataSingleValueLookupSchemaEntry(
                "col3",
                RowData.createFieldGetter(DataTypes.STRING().getLogicalType(), 2))
            );

        GenericRowData rowData =
            GenericRowData.of(
                GenericRowData.of(
                    StringData.fromString("val1"),
                    StringData.fromString("val2"),
                    StringData.fromString("val3")
                )
            );

        // WHEN
        List<LookupArg> lookupArgs = lookupSchemaEntry.convertToLookupArg(rowData);

        // THEN
        assertThat(lookupArgs).containsExactly(
            new LookupArg("col1", "val1"),
            new LookupArg("col2", "val2"),
            new LookupArg("col3", "val3")
        );
    }

    @Test
    public void testRowWithNestedRowValues() {

        // GIVEN
        RowTypeLookupSchemaEntry nestedRowLookupSchemaEntry = new RowTypeLookupSchemaEntry(
            "aRow",
            RowData.createFieldGetter(
                DataTypes.FIELD("nestedRow", DataTypes.ROW(
                        DataTypes.FIELD("col1", DataTypes.STRING()),
                        DataTypes.FIELD("col2", DataTypes.STRING())
                    )
                ).getDataType().getLogicalType(),
                0
            ))
            .addLookupEntry(new RowDataSingleValueLookupSchemaEntry(
                "col1",
                RowData.createFieldGetter(DataTypes.STRING().getLogicalType(), 0))
            )
            .addLookupEntry(new RowDataSingleValueLookupSchemaEntry(
                "col2",
                RowData.createFieldGetter(DataTypes.STRING().getLogicalType(), 1))
            );

        RowTypeLookupSchemaEntry rootSchemaEntry = new RowTypeLookupSchemaEntry(
            "aRow",
            RowData.createFieldGetter(
                DataTypes.ROW(
                        DataTypes.ROW(
                            DataTypes.FIELD("nestedRow", DataTypes.ROW(
                                DataTypes.FIELD("col1", DataTypes.STRING()),
                                DataTypes.FIELD("col2", DataTypes.STRING())
                            ))
                        ),
                        DataTypes.FIELD("col3", DataTypes.STRING()).getDataType()
                    )
                    .getLogicalType(),
                0)).addLookupEntry(nestedRowLookupSchemaEntry);

        GenericRowData rowData =
            GenericRowData.of(
                GenericRowData.of(
                    GenericRowData.of(
                        StringData.fromString("val1"),
                        StringData.fromString("val2")
                    )
                )
            );

        // WHEN
        List<LookupArg> lookupArgs = rootSchemaEntry.convertToLookupArg(rowData);

        // THEN
        assertThat(lookupArgs).containsExactly(
            new LookupArg("col1", "val1"),
            new LookupArg("col2", "val2")
        );
    }
}
