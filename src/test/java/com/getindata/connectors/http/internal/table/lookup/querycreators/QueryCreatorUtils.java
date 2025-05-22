package com.getindata.connectors.http.internal.table.lookup.querycreators;

import java.util.Collections;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.SerializationFormatFactory;

import com.getindata.connectors.http.internal.table.lookup.LookupRow;

public class QueryCreatorUtils {
    public static DynamicTableFactory.Context getTableContext(Configuration config,
                                                              ResolvedSchema resolvedSchema) {

        return new FactoryUtil.DefaultDynamicTableContext(
                ObjectIdentifier.of("default", "default", "test"),
                new ResolvedCatalogTable(
                        CatalogTable.of(
                                Schema.newBuilder().fromResolvedSchema(resolvedSchema).build(),
                                null,
                                Collections.emptyList(),
                                Collections.emptyMap()),
                        resolvedSchema),
                Collections.emptyMap(),
                config,
                Thread.currentThread().getContextClassLoader(),
                false
        );
    }
    public static SerializationSchema<RowData> getRowDataSerializationSchema(LookupRow lookupRow,
                                  DynamicTableFactory.Context dynamicTableFactoryContext,
                                  String formatIdentifier,
                                  QueryFormatAwareConfiguration queryFormatAwareConfiguration) {
        SerializationFormatFactory jsonFormatFactory =
                FactoryUtil.discoverFactory(
                        dynamicTableFactoryContext.getClassLoader(),
                        SerializationFormatFactory.class,
                        formatIdentifier
                );

        EncodingFormat<SerializationSchema<RowData>>
                encoder = jsonFormatFactory.createEncodingFormat(
                dynamicTableFactoryContext,
                queryFormatAwareConfiguration
        );

        final SerializationSchema<RowData> serializationSchema =
                encoder.createRuntimeEncoder(null, lookupRow.getLookupPhysicalRowDataType());
        return serializationSchema;
    }
}
