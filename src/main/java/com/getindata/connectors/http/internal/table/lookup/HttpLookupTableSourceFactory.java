package com.getindata.connectors.http.internal.table.lookup;

import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.DataTypes.Field;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DeserializationFormatFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.DataType;
import static org.apache.flink.table.api.DataTypes.FIELD;
import static org.apache.flink.table.types.utils.DataTypeUtils.removeTimeAttribute;

import com.getindata.connectors.http.internal.config.HttpConnectorConfigConstants;
import com.getindata.connectors.http.internal.utils.ConfigUtils;
import static com.getindata.connectors.http.internal.table.lookup.HttpLookupConnectorOptions.*;

public class HttpLookupTableSourceFactory implements DynamicTableSourceFactory {

    private static DataTypes.Field columnToField(Column column) {
        return FIELD(
        column.getName(),
            // only a column in a schema should have a time attribute,
            // a field should not propagate the attribute because it might be used in a
            // completely different context
            removeTimeAttribute(column.getDataType()));
    }

    public static DataType row(List<Field> fields) {
        return DataTypes.ROW(fields.toArray(new Field[0]));
    }

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        FactoryUtil.TableFactoryHelper helper =
            FactoryUtil.createTableFactoryHelper(this, context);

        ReadableConfig readableConfig = helper.getOptions();
        helper.validateExcept(
            // properties coming from org.apache.flink.table.api.config.ExecutionConfigOptions
            "table.",
            HttpConnectorConfigConstants.GID_CONNECTOR_HTTP,
            LOOKUP_REQUEST_FORMAT.key()
        );

        DecodingFormat<DeserializationSchema<RowData>> decodingFormat =
            helper.discoverDecodingFormat(
                DeserializationFormatFactory.class,
                FactoryUtil.FORMAT
            );

        HttpLookupConfig lookupConfig = getHttpLookupOptions(context, readableConfig);

        ResolvedSchema resolvedSchema = context.getCatalogTable().getResolvedSchema();

        DataType physicalRowDataType =
            toRowDataType(resolvedSchema.getColumns(), Column::isPhysical);

        return new HttpLookupTableSource(
            physicalRowDataType,
            lookupConfig,
            decodingFormat
        );
    }

    @Override
    public String factoryIdentifier() {
        return "rest-lookup";
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return Set.of(URL, FactoryUtil.FORMAT);
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return Set.of(URL_ARGS, ASYNC_POLLING, LOOKUP_METHOD);
    }

    private HttpLookupConfig getHttpLookupOptions(Context context, ReadableConfig readableConfig) {

        Properties httpConnectorProperties =
            ConfigUtils.getHttpConnectorProperties(context.getCatalogTable().getOptions());

        return HttpLookupConfig.builder()
            .lookupMethod(readableConfig.get(LOOKUP_METHOD))
            .url(readableConfig.get(URL))
            .useAsync(readableConfig.get(ASYNC_POLLING))
            .properties(httpConnectorProperties)
            .readableConfig(readableConfig)
            .build();
    }

    // TODO verify this since we are on 1.15 now.
    // Backport from Flink 1.15-Master
    private DataType toRowDataType(List<Column> columns, Predicate<Column> columnPredicate) {
        return columns.stream()
            .filter(columnPredicate)
            .map(HttpLookupTableSourceFactory::columnToField)
            .collect(
                Collectors.collectingAndThen(Collectors.toList(),
                    HttpLookupTableSourceFactory::row))
            // the row should never be null
            .notNull();
    }

    // Backport End
}
