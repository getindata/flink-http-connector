package com.getindata.connectors.http.internal.table.sink;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

/**
 * Table API options for {@link HttpDynamicSink}.
 */
public class HttpDynamicSinkConnectorOptions {

    public static final ConfigOption<String> URL =
        ConfigOptions.key("url").stringType().noDefaultValue()
            .withDescription("The HTTP endpoint URL.");

    public static final ConfigOption<String> URL_ARGS =
        ConfigOptions.key("url-args")
            .stringType()
            .noDefaultValue()
            .withDescription("The arguments that should be used for HTTP Request.");

    public static final ConfigOption<String> INSERT_METHOD =
        ConfigOptions.key("insert-method")
            .stringType()
            .defaultValue("POST")
            .withDescription("Method used for requests built from SQL's INSERT.");
}
