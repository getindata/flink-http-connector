package com.getindata.connectors.http.internal.table.sink;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.connector.base.DeliveryGuarantee;

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
            .defaultValue(DeliveryGuarantee.NONE)
            .withDescription("Defines the delivery semantic for the HTTP sink. " +
                "Accepted enumerations are 'at-least-once', and 'none'. " +
                "'exactly-once' semantic is not supported.");
}
