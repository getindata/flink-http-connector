/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.http.table.sink;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.base.table.AsyncDynamicTableSinkFactory;
import org.apache.flink.connector.base.table.sink.options.AsyncSinkConfigurationValidator;
import org.apache.flink.connector.http.HttpPostRequestCallbackFactory;
import org.apache.flink.connector.http.config.HttpConnectorConfigConstants;
import org.apache.flink.connector.http.sink.httpclient.HttpRequest;
import org.apache.flink.connector.http.utils.ConfigUtils;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.factories.FactoryUtil;

import java.util.Properties;
import java.util.Set;

import static org.apache.flink.connector.http.table.sink.HttpDynamicSinkConnectorOptions.INSERT_METHOD;
import static org.apache.flink.connector.http.table.sink.HttpDynamicSinkConnectorOptions.REQUEST_CALLBACK_IDENTIFIER;
import static org.apache.flink.connector.http.table.sink.HttpDynamicSinkConnectorOptions.URL;

/** Factory for creating {@link HttpDynamicSink}. */
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
                        // properties coming from
                        // org.apache.flink.table.api.config.ExecutionConfigOptions
                        "table.", HttpConnectorConfigConstants.FLINK_CONNECTOR_HTTP);
        validateHttpSinkOptions(tableOptions);

        Properties asyncSinkProperties =
                new AsyncSinkConfigurationValidator(tableOptions).getValidatedConfigurations();

        // generics type erasure, so we have to do an unchecked cast
        final HttpPostRequestCallbackFactory<HttpRequest> postRequestCallbackFactory =
                FactoryUtil.discoverFactory(
                        context.getClassLoader(),
                        HttpPostRequestCallbackFactory.class, // generics type erasure
                        tableOptions.get(REQUEST_CALLBACK_IDENTIFIER));

        Properties httpConnectorProperties =
                ConfigUtils.getHttpConnectorProperties(context.getCatalogTable().getOptions());

        HttpDynamicSink.HttpDynamicTableSinkBuilder builder =
                new HttpDynamicSink.HttpDynamicTableSinkBuilder()
                        .setTableOptions(tableOptions)
                        .setEncodingFormat(factoryContext.getEncodingFormat())
                        .setHttpPostRequestCallback(
                                postRequestCallbackFactory.createHttpPostRequestCallback())
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
        tableOptions
                .getOptional(INSERT_METHOD)
                .ifPresent(
                        insertMethod -> {
                            if (!Set.of("POST", "PUT").contains(insertMethod)) {
                                throw new IllegalArgumentException(
                                        String.format(
                                                "Invalid option '%s'. It is expected to be either 'POST' or 'PUT'.",
                                                INSERT_METHOD.key()));
                            }
                        });
    }
}
