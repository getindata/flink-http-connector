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

package org.apache.flink.connector.http.sink.httpclient;

import org.apache.flink.connector.http.config.ConfigException;
import org.apache.flink.connector.http.config.HttpConnectorConfigConstants;
import org.apache.flink.connector.http.utils.JavaNetHttpClientFactory;
import org.apache.flink.connector.http.utils.ThreadUtils;
import org.apache.flink.util.StringUtils;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;

import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/** Batch request submitter factory. */
public class BatchRequestSubmitterFactory implements RequestSubmitterFactory {

    // TODO Add this property to config. Make sure to add note in README.md that will describe that
    //  any value greater than one will break order of messages.
    static final int HTTP_CLIENT_THREAD_POOL_SIZE = 1;

    private final String maxBatchSize;

    public BatchRequestSubmitterFactory(int maxBatchSize) {
        if (maxBatchSize < 1) {
            throw new IllegalArgumentException(
                    "Batch Request submitter batch size must be greater than zero.");
        }
        this.maxBatchSize = String.valueOf(maxBatchSize);
    }

    @Override
    public BatchRequestSubmitter createSubmitter(Properties properties, String[] headersAndValues) {
        String batchRequestSize =
                properties.getProperty(HttpConnectorConfigConstants.SINK_HTTP_BATCH_REQUEST_SIZE);
        if (StringUtils.isNullOrWhitespaceOnly(batchRequestSize)) {
            properties.setProperty(
                    HttpConnectorConfigConstants.SINK_HTTP_BATCH_REQUEST_SIZE, maxBatchSize);
        } else {
            try {
                // TODO Create property validator someday.
                int batchSize = Integer.parseInt(batchRequestSize);
                if (batchSize < 1) {
                    throw new ConfigException(
                            String.format(
                                    "Property %s must be greater than 0 but was: %s",
                                    HttpConnectorConfigConstants.SINK_HTTP_BATCH_REQUEST_SIZE,
                                    batchRequestSize));
                }
            } catch (NumberFormatException e) {
                // TODO Create property validator someday.
                throw new ConfigException(
                        String.format(
                                "Property %s must be an integer but was: %s",
                                HttpConnectorConfigConstants.SINK_HTTP_BATCH_REQUEST_SIZE,
                                batchRequestSize),
                        e);
            }
        }

        ExecutorService httpClientExecutor =
                Executors.newFixedThreadPool(
                        HTTP_CLIENT_THREAD_POOL_SIZE,
                        new ExecutorThreadFactory(
                                "http-sink-client-batch-request-worker",
                                ThreadUtils.LOGGING_EXCEPTION_HANDLER));

        return new BatchRequestSubmitter(
                properties,
                headersAndValues,
                JavaNetHttpClientFactory.createClient(properties, httpClientExecutor));
    }
}
