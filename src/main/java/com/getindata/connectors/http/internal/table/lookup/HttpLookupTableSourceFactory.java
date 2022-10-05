package com.getindata.connectors.http.internal.table.lookup;

import java.util.Arrays;
import java.util.Collections;
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

import com.getindata.connectors.http.LookupQueryCreatorFactory;
import com.getindata.connectors.http.internal.HeaderPreprocessor;
import com.getindata.connectors.http.internal.PollingClientFactory;
import com.getindata.connectors.http.internal.config.HttpConnectorConfigConstants;
import com.getindata.connectors.http.internal.table.lookup.querycreators.GenericGetQueryCreatorFactory;
import com.getindata.connectors.http.internal.table.lookup.querycreators.GenericJsonQueryCreatorFactory;
import com.getindata.connectors.http.internal.utils.ConfigUtils;
import com.getindata.connectors.http.internal.utils.HttpHeaderUtils;
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
        helper.validateExcept(HttpConnectorConfigConstants.GID_CONNECTOR_HTTP);

        DecodingFormat<DeserializationSchema<RowData>> decodingFormat =
            helper.discoverDecodingFormat(
                DeserializationFormatFactory.class,
                FactoryUtil.FORMAT
            );

        String lookupMethod = readableConfig.get(LOOKUP_METHOD);

        LookupQueryCreatorFactory lookupQueryCreatorFactory =
            FactoryUtil.discoverFactory(
                context.getClassLoader(),
                LookupQueryCreatorFactory.class,
                readableConfig.getOptional(LOOKUP_QUERY_CREATOR_IDENTIFIER).orElse(
                    (lookupMethod.equalsIgnoreCase("GET") ?
                        GenericGetQueryCreatorFactory.IDENTIFIER :
                        GenericJsonQueryCreatorFactory.IDENTIFIER)
                )
            );

        HttpLookupConfig lookupConfig = getHttpLookupOptions(context, readableConfig);

        // TODO Consider this to be injected as method argument or factory field
        //  so user could set this using API.
        PollingClientFactory<RowData> pollingClientFactory =
            createPollingClientFactory(lookupMethod, lookupQueryCreatorFactory, lookupConfig);

        ResolvedSchema resolvedSchema = context.getCatalogTable().getResolvedSchema();

        DataType physicalRowDataType =
            toRowDataType(resolvedSchema.getColumns(), Column::isPhysical);

        return new HttpLookupTableSource(
            physicalRowDataType,
            pollingClientFactory,
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

    private PollingClientFactory<RowData> createPollingClientFactory(
        String lookupMethod,
        LookupQueryCreatorFactory lookupQueryCreatorFactory,
        HttpLookupConfig lookupConfig) {

        HeaderPreprocessor headerPreprocessor = HttpHeaderUtils.createDefaultHeaderPreprocessor();

        HttpRequestFactory requestFactory = (lookupMethod.equalsIgnoreCase("GET")) ?
            new GetRequestFactory(
                lookupQueryCreatorFactory.createLookupQueryCreator(),
                headerPreprocessor,
                lookupConfig) :
            new BodyBasedRequestFactory(
                lookupMethod,
                lookupQueryCreatorFactory.createLookupQueryCreator(),
                headerPreprocessor,
                lookupConfig
            );

        return new JavaNetHttpPollingClientFactory(requestFactory);
    }

    private HttpLookupConfig getHttpLookupOptions(Context context, ReadableConfig config) {

        Properties httpConnectorProperties =
            ConfigUtils.getHttpConnectorProperties(context.getCatalogTable().getOptions());

        return HttpLookupConfig.builder()
            .url(config.get(URL))
            .arguments(convertArguments(config.get((URL_ARGS))))
            .useAsync(config.get(ASYNC_POLLING))
            .properties(httpConnectorProperties)
            .build();
    }

    private List<String> convertArguments(String arguments) {

        if (arguments == null || arguments.isEmpty() || arguments.isBlank()) {
            return Collections.emptyList();
        }

        return Arrays.stream(arguments.split(";")).map(String::trim).collect(Collectors.toList());
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
