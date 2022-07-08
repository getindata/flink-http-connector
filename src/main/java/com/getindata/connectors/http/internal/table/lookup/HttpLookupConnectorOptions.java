package com.getindata.connectors.http.internal.table.lookup;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

public class HttpLookupConnectorOptions {

    public static final String FIELD = "field";
    public static final String PATH = "path";
    public static final String ROOT = "root";

    public static final ConfigOption<String> URL =
        ConfigOptions.key("url")
            .stringType()
            .noDefaultValue()
            .withDescription("The HTTP endpoint URL.");

    public static final ConfigOption<String> URL_ARGS =
        ConfigOptions.key("url-args")
            .stringType()
            .noDefaultValue()
            .withDescription("The arguments that should be used for HTTP GET Request.");

    public static final ConfigOption<Boolean> ASYNC_POLLING =
        ConfigOptions.key("asyncPolling")
            .booleanType()
            .defaultValue(false)
            .withDescription("Whether to use Sync and Async polling mechanism");

    public static final ConfigOption<String> FIELD_ALIAS =
        ConfigOptions.key(String.format("%s.#.%s", FIELD, PATH))
            .stringType()
            .noDefaultValue()
            .withDescription("The JsonPath that represents alias field.");

    public static final ConfigOption<String> ROOT_NODE =
        ConfigOptions.key(ROOT)
            .stringType()
            .noDefaultValue()
            .withDescription("Root Json Node for entire table.");
}
