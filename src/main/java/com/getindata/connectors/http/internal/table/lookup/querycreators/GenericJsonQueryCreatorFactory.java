package com.getindata.connectors.http.internal.table.lookup.querycreators;

import java.util.Set;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.SerializationFormatFactory;

import com.getindata.connectors.http.LookupQueryCreator;
import com.getindata.connectors.http.LookupQueryCreatorFactory;
import com.getindata.connectors.http.internal.table.lookup.LookupRow;
import static com.getindata.connectors.http.internal.table.lookup.HttpLookupConnectorOptions.LOOKUP_REQUEST_FORMAT;

/**
 * Factory for creating {@link GenericJsonQueryCreatorFactory}.
 */
public class GenericJsonQueryCreatorFactory implements LookupQueryCreatorFactory {

    public static final String IDENTIFIER = "generic-json-query";

    @Override
    public LookupQueryCreator createLookupQueryCreator(
            ReadableConfig readableConfig,
            LookupRow lookupRow,
            DynamicTableFactory.Context dynamicTableFactoryContext) {

        String formatIdentifier = readableConfig.get(LOOKUP_REQUEST_FORMAT);
        SerializationFormatFactory jsonFormatFactory =
            FactoryUtil.discoverFactory(
                dynamicTableFactoryContext.getClassLoader(),
                SerializationFormatFactory.class,
                formatIdentifier
            );
        QueryFormatAwareConfiguration queryFormatAwareConfiguration =
            new QueryFormatAwareConfiguration(
                LOOKUP_REQUEST_FORMAT.key() + "." + formatIdentifier,
                (Configuration) readableConfig);
        EncodingFormat<SerializationSchema<RowData>>
            encoder = jsonFormatFactory.createEncodingFormat(
            dynamicTableFactoryContext,
            queryFormatAwareConfiguration
        );

        SerializationSchema<RowData> serializationSchema =
            encoder.createRuntimeEncoder(null, lookupRow.getLookupPhysicalRowDataType());

        return new GenericJsonQueryCreator(serializationSchema);
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return Set.of();
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return Set.of();
    }
}
