package com.getindata.connectors.http.internal.table.lookup;

import java.util.List;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;

import com.getindata.connectors.http.LookupArg;

class RowDataSingleValueLookupSchemaEntryTest {

    // TODO Convert this to parametrized test and check all Flink types (Int, String etc).
    @Test
    public void shouldConvertFromSingleValue() {

        RowDataSingleValueLookupSchemaEntry entry = new RowDataSingleValueLookupSchemaEntry(
            "col1",
            RowData.createFieldGetter(DataTypes.BOOLEAN().getLogicalType(), 0)
        );

        List<LookupArg> lookupArgs = entry.convertToLookupArg(GenericRowData.of(true));

        assertThat(lookupArgs).containsExactly(new LookupArg("col1", "true"));
    }
}
