package com.getindata.connectors.http.internal.table.lookup;

import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.DataTypes.Field;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.lookup.LookupOptions;
import org.apache.flink.table.connector.source.lookup.cache.DefaultLookupCache;
import org.apache.flink.table.connector.source.lookup.cache.LookupCache;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DeserializationFormatFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.DataType;
import static org.apache.flink.table.api.DataTypes.FIELD;
import static org.apache.flink.table.types.utils.DataTypeUtils.removeTimeAttribute;

import com.getindata.connectors.http.HttpPostRequestCallbackFactory;
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
    public DynamicTableSource createDynamicTableSource(Context dynamicTableContext) {
        FactoryUtil.TableFactoryHelper helper =
            FactoryUtil.createTableFactoryHelper(this, dynamicTableContext);

        ReadableConfig readable = helper.getOptions();
        helper.validateExcept(
            // properties coming from org.apache.flink.table.api.config.ExecutionConfigOptions
            "table.",
            HttpConnectorConfigConstants.GID_CONNECTOR_HTTP,
            LOOKUP_REQUEST_FORMAT.key()
        );
        validateHttpLookupSourceOptions(readable);

        DecodingFormat<DeserializationSchema<RowData>> decodingFormat =
            helper.discoverDecodingFormat(
                DeserializationFormatFactory.class,
                FactoryUtil.FORMAT
            );

        HttpLookupConfig lookupConfig = getHttpLookupOptions(dynamicTableContext, readable);

        ResolvedSchema resolvedSchema = dynamicTableContext.getCatalogTable().getResolvedSchema();

        DataType physicalRowDataType =
            toRowDataType(resolvedSchema.getColumns(), Column::isPhysical);

        return new HttpLookupTableSource(
            physicalRowDataType,
            lookupConfig,
            decodingFormat,
            dynamicTableContext,
            getLookupCache(readable)
        );
    }

    protected void validateHttpLookupSourceOptions(ReadableConfig tableOptions)
            throws IllegalArgumentException {
        // ensure that there is an OIDC token request if we have an OIDC token endpoint
        tableOptions.getOptional(SOURCE_LOOKUP_OIDC_AUTH_TOKEN_ENDPOINT_URL).ifPresent(url -> {
            if (tableOptions.getOptional(SOURCE_LOOKUP_OIDC_AUTH_TOKEN_REQUEST).isEmpty()) {
                throw new IllegalArgumentException("Config option " +
                    SOURCE_LOOKUP_OIDC_AUTH_TOKEN_REQUEST.key() + " is required, if " +
                        SOURCE_LOOKUP_OIDC_AUTH_TOKEN_ENDPOINT_URL.key() + " is configured.");
            }
        });
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
        return Set.of(
            URL_ARGS,
            ASYNC_POLLING,
            LOOKUP_METHOD,
            REQUEST_CALLBACK_IDENTIFIER,

            LookupOptions.CACHE_TYPE,
            LookupOptions.PARTIAL_CACHE_EXPIRE_AFTER_ACCESS,
            LookupOptions.PARTIAL_CACHE_EXPIRE_AFTER_WRITE,
            LookupOptions.PARTIAL_CACHE_MAX_ROWS,
            LookupOptions.PARTIAL_CACHE_CACHE_MISSING_KEY,

            SOURCE_LOOKUP_OIDC_AUTH_TOKEN_EXPIRY_REDUCTION,
            SOURCE_LOOKUP_OIDC_AUTH_TOKEN_REQUEST,
            SOURCE_LOOKUP_OIDC_AUTH_TOKEN_ENDPOINT_URL,

            LookupOptions.MAX_RETRIES,
            SOURCE_LOOKUP_RETRY_STRATEGY,
            SOURCE_LOOKUP_RETRY_FIXED_DELAY_DELAY,
            SOURCE_LOOKUP_RETRY_EXPONENTIAL_DELAY_INITIAL_BACKOFF,
            SOURCE_LOOKUP_RETRY_EXPONENTIAL_DELAY_MULTIPLIER,
            SOURCE_LOOKUP_RETRY_EXPONENTIAL_DELAY_MAX_BACKOFF,

            SOURCE_LOOKUP_HTTP_SUCCESS_CODES,
            SOURCE_LOOKUP_HTTP_RETRY_CODES,
            SOURCE_LOOKUP_HTTP_IGNORED_RESPONSE_CODES,

            SOURCE_LOOKUP_CONNECTION_TIMEOUT        // TODO: add request timeout from properties
        );
    }

    private HttpLookupConfig getHttpLookupOptions(Context context, ReadableConfig readableConfig) {

        Properties httpConnectorProperties =
            ConfigUtils.getHttpConnectorProperties(context.getCatalogTable().getOptions());

        final HttpPostRequestCallbackFactory<HttpLookupSourceRequestEntry>
                postRequestCallbackFactory =
                    FactoryUtil.discoverFactory(
                        context.getClassLoader(),
                        HttpPostRequestCallbackFactory.class,
                        readableConfig.get(REQUEST_CALLBACK_IDENTIFIER)
        );

        return HttpLookupConfig.builder()
            .lookupMethod(readableConfig.get(LOOKUP_METHOD))
            .url(readableConfig.get(URL))
            .useAsync(readableConfig.get(ASYNC_POLLING))
            .properties(httpConnectorProperties)
            .readableConfig(readableConfig)
            .httpPostRequestCallback(postRequestCallbackFactory.createHttpPostRequestCallback())
            .build();
    }

    @Nullable
    private LookupCache getLookupCache(ReadableConfig tableOptions) {
        LookupCache cache = null;
        // Do not support legacy cache options
        if (tableOptions
                .get(LookupOptions.CACHE_TYPE)
                .equals(LookupOptions.LookupCacheType.PARTIAL)) {
            cache = DefaultLookupCache.fromConfig(tableOptions);
        }
        return cache;
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
