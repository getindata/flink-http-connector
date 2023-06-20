package com.getindata.connectors.http.internal.sink.httpclient;

import java.util.Properties;

import org.apache.flink.util.StringUtils;

import com.getindata.connectors.http.internal.config.ConfigException;
import com.getindata.connectors.http.internal.config.HttpConnectorConfigConstants;

public class BatchRequestSubmitterFactory implements RequestSubmitterFactory {

    private final String maxBatchSize;

    public BatchRequestSubmitterFactory(int maxBatchSize) {
        this.maxBatchSize = String.valueOf(maxBatchSize);
    }

    @Override
    public RequestSubmitter createSubmitter(Properties properties, String[] headersAndValues) {
        String batchRequestSize =
            properties.getProperty(HttpConnectorConfigConstants.SINK_HTTP_BATCH_REQUEST_SIZE);
        if (StringUtils.isNullOrWhitespaceOnly(batchRequestSize)) {
            properties.setProperty(
                HttpConnectorConfigConstants.SINK_HTTP_BATCH_REQUEST_SIZE,
                maxBatchSize
            );
        } else {
            try {
                // TODO Create property validator someday.
                int batchSize = Integer.parseInt(batchRequestSize);
                if (batchSize < 1) {
                    throw new ConfigException(
                        String.format("Property %s must be greater than 0 but was: %s",
                            HttpConnectorConfigConstants.SINK_HTTP_BATCH_REQUEST_SIZE,
                            batchRequestSize)
                    );
                }
            } catch (NumberFormatException e) {
                // TODO Create property validator someday.
                throw new ConfigException(
                    String.format("Property %s must be an integer but was: %s",
                        HttpConnectorConfigConstants.SINK_HTTP_BATCH_REQUEST_SIZE,
                        batchRequestSize),
                    e
                );
            }
        }
        return new BatchRequestSubmitter(properties, headersAndValues);
    }
}
