package com.getindata.connectors.http.internal.table.sink;

import java.util.Properties;
import java.util.Set;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.base.table.AsyncDynamicTableSinkFactory;
import org.apache.flink.connector.base.table.sink.options.AsyncSinkConfigurationValidator;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.factories.FactoryUtil;

import com.getindata.connectors.http.HttpPostRequestCallbackFactory;
import com.getindata.connectors.http.internal.config.HttpConnectorConfigConstants;
import com.getindata.connectors.http.internal.sink.httpclient.HttpRequest;
import com.getindata.connectors.http.internal.utils.ConfigUtils;
import static com.getindata.connectors.http.internal.table.sink.HttpDynamicSinkConnectorOptions.*;

/**
 * Factory for creating {@link HttpDynamicSink}.
 */
public class HttpDynamicTableSinkFactory extends AsyncDynamicTableSinkFactory {

    public static final String IDENTIFIER = "http-sink";

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        final AsyncDynamicSinkContext factoryContext = new AsyncDynamicSinkContext(this, context);

        // This is actually same as calling helper.getOptions();
        ReadableConfig tableOptions = factoryContext.getTableOptions();

        // Validate configuration
        FactoryUtil.createTableFactoryHelper(this, context)
            .validateExcept(
                // properties coming from org.apache.flink.table.api.config.ExecutionConfigOptions
                "table.",
                HttpConnectorConfigConstants.GID_CONNECTOR_HTTP
            );
        validateHttpSinkOptions(tableOptions);

        Properties asyncSinkProperties =
            new AsyncSinkConfigurationValidator(tableOptions).getValidatedConfigurations();

        // generics type erasure, so we have to do an unchecked cast
        final HttpPostRequestCallbackFactory<HttpRequest> postRequestCallbackFactory =
            FactoryUtil.discoverFactory(
                context.getClassLoader(),
                HttpPostRequestCallbackFactory.class,  // generics type erasure
                tableOptions.get(REQUEST_CALLBACK_IDENTIFIER)
            );

        Properties httpConnectorProperties =
            ConfigUtils.getHttpConnectorProperties(context.getCatalogTable().getOptions());

        HttpDynamicSink.HttpDynamicTableSinkBuilder builder =
            new HttpDynamicSink.HttpDynamicTableSinkBuilder()
                .setTableOptions(tableOptions)
                .setEncodingFormat(factoryContext.getEncodingFormat())
                .setHttpPostRequestCallback(
                    postRequestCallbackFactory.createHttpPostRequestCallback()
                )
                .setConsumedDataType(factoryContext.getPhysicalDataType())
                .setProperties(httpConnectorProperties);
        addAsyncOptionsToBuilder(asyncSinkProperties, builder);

        return builder.build();
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return Set.of(URL, FactoryUtil.FORMAT);
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        var options = super.optionalOptions();
        options.add(INSERT_METHOD);
        options.add(REQUEST_CALLBACK_IDENTIFIER);
        return options;
    }

    private void validateHttpSinkOptions(ReadableConfig tableOptions)
        throws IllegalArgumentException {
        tableOptions.getOptional(INSERT_METHOD).ifPresent(insertMethod -> {
            if (!Set.of("POST", "PUT").contains(insertMethod)) {
                throw new IllegalArgumentException(
                    String.format(
                        "Invalid option '%s'. It is expected to be either 'POST' or 'PUT'.",
                        INSERT_METHOD.key()
                    ));
            }
        });
    }
}
