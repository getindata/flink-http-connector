package com.getindata.connectors.http.internal.table.sink;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.connector.base.DeliveryGuarantee;

import com.getindata.connectors.http.internal.config.HttpConnectorConfigConstants;
import static com.getindata.connectors.http.internal.config.HttpConnectorConfigConstants.SINK_REQUEST_CALLBACK_IDENTIFIER;

/**
 * Table API options for {@link HttpDynamicSink}.
 */
public class HttpDynamicSinkConnectorOptions {

    public static final ConfigOption<String> URL =
        ConfigOptions.key("url").stringType().noDefaultValue()
            .withDescription("The HTTP endpoint URL.");

    public static final ConfigOption<String> INSERT_METHOD =
        ConfigOptions.key("insert-method")
            .stringType()
            .defaultValue("POST")
            .withDescription("Method used for requests built from SQL's INSERT.");

    public static final ConfigOption<String> REQUEST_CALLBACK_IDENTIFIER =
        ConfigOptions.key(SINK_REQUEST_CALLBACK_IDENTIFIER)
            .stringType()
            .defaultValue(Slf4jHttpPostRequestCallbackFactory.IDENTIFIER);

    public static final ConfigOption<DeliveryGuarantee> DELIVERY_GUARANTEE =
        ConfigOptions.key("sink.delivery-guarantee")
            .enumType(DeliveryGuarantee.class)
            .defaultValue(DeliveryGuarantee.AT_LEAST_ONCE)
            .withDescription("Defines the delivery semantic for the HTTP sink. " +
                    "Accepted enumerations are 'at-least-once', and 'none'. " +
                    "'exactly-once' semantic is not supported.");

    public static final ConfigOption<String> SINK_HTTP_SUCCESS_CODES =
        ConfigOptions.key(HttpConnectorConfigConstants.SINK_SUCCESS_CODES)
            .stringType()
            .defaultValue("2XX")
            .withDescription("Comma separated HTTP status codes treated as successful responses. " +
                "Supports range masks (e.g. '2XX') and exclusions with '!'. " +
                "Defaults to '2XX'.");

    public static final ConfigOption<String> SINK_HTTP_RETRY_CODES =
        ConfigOptions.key(HttpConnectorConfigConstants.SINK_RETRY_CODES)
            .stringType()
            .defaultValue("500,503,504")
            .withDescription("Comma separated HTTP status codes treated as transient errors " +
                "that trigger automatic retries when sink.delivery-guarantee is 'at-least-once'. " +
                "Supports range masks (e.g. '5XX') and exclusions with '!'. " +
                "Defaults to '500,503,504'.");

    public static final ConfigOption<String> SINK_HTTP_IGNORED_RESPONSE_CODES =
        ConfigOptions.key(HttpConnectorConfigConstants.SINK_IGNORE_RESPONSE_CODES)
            .stringType()
            .defaultValue("")
            .withDescription("Comma separated HTTP status codes whose response body is ignored " +
                "but treated as successful. Supports range masks and exclusions with '!'.");
}
