package com.getindata.connectors.http.internal.table.lookup.querycreators;

import java.util.Collections;
import java.util.Set;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DynamicTableFactory.Context;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.SerializationFormatFactory;
import org.apache.flink.table.factories.TestFormatFactory.EncodingFormatMock;

public class CustomFormatFactory implements SerializationFormatFactory {

    public static final String IDENTIFIER = "query-creator-test-format";
    public static final String REQUIRED_OPTION = "required-option-one";
    static boolean requiredOptionsWereUsed = false;

    @Override
    public EncodingFormat<SerializationSchema<RowData>> createEncodingFormat(
        Context context,
        ReadableConfig readableConfig) {
        FactoryUtil.validateFactoryOptions(this, readableConfig);
        return new EncodingFormatMock(",");
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        requiredOptionsWereUsed = true;
        return Set.of(ConfigOptions.key(REQUIRED_OPTION).stringType().noDefaultValue());
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return Collections.emptySet();
    }
}
