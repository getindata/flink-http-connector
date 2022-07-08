package com.getindata.connectors.http.internal.table.lookup;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;


import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.DataTypes.Field;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.DataType;
import static org.apache.flink.table.api.DataTypes.FIELD;
import static org.apache.flink.table.types.utils.DataTypeUtils.removeTimeAttribute;

import com.getindata.connectors.http.internal.PollingClientFactory;
import static com.getindata.connectors.http.internal.table.lookup.HttpLookupConnectorOptions.ASYNC_POLLING;
import static com.getindata.connectors.http.internal.table.lookup.HttpLookupConnectorOptions.FIELD;
import static com.getindata.connectors.http.internal.table.lookup.HttpLookupConnectorOptions.FIELD_ALIAS;
import static com.getindata.connectors.http.internal.table.lookup.HttpLookupConnectorOptions.PATH;
import static com.getindata.connectors.http.internal.table.lookup.HttpLookupConnectorOptions.ROOT_NODE;
import static com.getindata.connectors.http.internal.table.lookup.HttpLookupConnectorOptions.URL;
import static com.getindata.connectors.http.internal.table.lookup.HttpLookupConnectorOptions.URL_ARGS;

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
        final FactoryUtil.TableFactoryHelper helper =
            FactoryUtil.createTableFactoryHelper(this, context);

        ReadableConfig readableConfig = helper.getOptions();

        ResolvedSchema resolvedSchema = context.getCatalogTable().getResolvedSchema();
        DataType physicalRowDataType =
            toRowDataType(resolvedSchema.getColumns(), Column::isPhysical);

        PollingClientFactory<RowData> factory = new RestTablePollingClientFactory();
        HttpLookupConfig lookupConfig = getHttpLookupOptions(readableConfig, physicalRowDataType);

        validateOptions(context, readableConfig, physicalRowDataType);

        return new HttpLookupTableSource(physicalRowDataType, factory, lookupConfig);
    }

    @Override
    public String factoryIdentifier() {
        return "rest-lookup";
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return Set.of(URL);
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return Set.of(URL_ARGS, ROOT_NODE, FIELD_ALIAS, ASYNC_POLLING);
    }

    private HttpLookupConfig getHttpLookupOptions(
        ReadableConfig config, DataType physicalRowDataType) {
        List<String> fieldNames = TableSourceHelper.getFieldNames(physicalRowDataType);
        return HttpLookupConfig.builder()
            .url(config.get(URL))
            .root(config.get(ROOT_NODE))
            .arguments(convertArguments(config.get((URL_ARGS))))
            .columnNames(fieldNames)
            .aliasPaths(buildAliasPaths(fieldNames, config))
            .useAsync(config.get(ASYNC_POLLING))
            .build();
    }

    // TODO FIX There is something ugly here. Verify with DataGenTableSourceFactory and refactor.
    private void validateOptions(
        Context context, ReadableConfig readableConfig, DataType physicalRowDataType) {
        Set<ConfigOption<?>> optionalAliasPaths =
            getAliasConfigs(TableSourceHelper.getFieldNames(physicalRowDataType), readableConfig);
        Set<ConfigOption<?>> allOptions = new HashSet<>();
        allOptions.addAll(optionalAliasPaths);
        allOptions.addAll(optionalOptions());
        allOptions.addAll(requiredOptions());
        allOptions.add(ConfigOptions.key("connector").stringType().noDefaultValue());

        FactoryUtil.validateFactoryOptions(requiredOptions(), allOptions, readableConfig);

        Configuration options = new Configuration();
        context.getCatalogTable().getOptions().forEach(options::setString);
        Set<String> consumedOptionKeys = new HashSet<>();
        allOptions.stream().map(ConfigOption::key).forEach(consumedOptionKeys::add);
        FactoryUtil.validateUnconsumedKeys(factoryIdentifier(), options.keySet(),
            consumedOptionKeys);
    }

    // TODO Verify if we have to create ConfigOptions
    private Set<ConfigOption<?>> getAliasConfigs(List<String> fieldNames, ReadableConfig config) {
        Set<ConfigOption<?>> aliasConfigs = new HashSet<>();

        for (String fieldName : fieldNames) {
            ConfigOption<String> pathWithAliasOption =
                ConfigOptions.key(String.format("%s.%s.%s", FIELD, fieldName, PATH))
                    .stringType()
                    .noDefaultValue();

            Optional<String> pathWithAlias = config.getOptional(pathWithAliasOption);
            pathWithAlias.ifPresent(aliasPath -> aliasConfigs.add(pathWithAliasOption));
        }

        return aliasConfigs;
    }

    private Map<String, String> buildAliasPaths(List<String> fieldNames, ReadableConfig config) {

        Map<String, String> aliasPaths = new HashMap<>();

        for (String fieldName : fieldNames) {

            ConfigOption<String> pathWithAliasOption =
                ConfigOptions.key(String.format("%s.%s.%s", FIELD, fieldName, PATH))
                    .stringType()
                    .noDefaultValue();

            Optional<String> pathWithAlias = config.getOptional(pathWithAliasOption);
            pathWithAlias.ifPresent(aliasPath -> aliasPaths.put(fieldName, aliasPath));
        }

        return aliasPaths;
    }

    private List<String> convertArguments(String arguments) {

        if (arguments == null || arguments.isEmpty() || arguments.isBlank()) {
            return Collections.emptyList();
        }

        return Arrays.stream(arguments.split(";")).map(String::trim).collect(Collectors.toList());
    }

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
