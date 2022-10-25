package com.getindata.connectors.http.internal.table.lookup;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

import static com.getindata.connectors.http.internal.config.HttpConnectorConfigConstants.SOURCE_LOOKUP_QUERY_CREATOR_IDENTIFIER;

public class HttpLookupConnectorOptions {

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

    public static final ConfigOption<String> LOOKUP_METHOD =
        ConfigOptions.key("lookup-method")
            .stringType()
            .defaultValue("GET")
            .withDescription("Method used for REST executed by lookup connector.");

    public static final ConfigOption<String> LOOKUP_QUERY_CREATOR_IDENTIFIER =
        ConfigOptions.key(SOURCE_LOOKUP_QUERY_CREATOR_IDENTIFIER)
            .stringType()
            .noDefaultValue();

    public static final ConfigOption<String> LOOKUP_REQUEST_FORMAT =
        ConfigOptions.key("lookup-request.format")
            .stringType()
            .defaultValue("json");
}
