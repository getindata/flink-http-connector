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

import org.apache.flink.connector.http.config.HttpConnectorConfigConstants;
import org.apache.flink.connector.http.utils.ThreadUtils;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;

import java.net.http.HttpClient;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/** Abstract request submitter. */
public abstract class AbstractRequestSubmitter implements RequestSubmitter {

    protected static final int HTTP_CLIENT_PUBLISHING_THREAD_POOL_SIZE = 1;

    protected static final String DEFAULT_REQUEST_TIMEOUT_SECONDS = "30";

    /** Thread pool to handle HTTP response from HTTP client. */
    protected final ExecutorService publishingThreadPool;

    protected final int httpRequestTimeOutSeconds;

    protected final String[] headersAndValues;

    protected final HttpClient httpClient;

    public AbstractRequestSubmitter(
            Properties properties, String[] headersAndValues, HttpClient httpClient) {

        this.headersAndValues = headersAndValues;
        this.publishingThreadPool =
                Executors.newFixedThreadPool(
                        HTTP_CLIENT_PUBLISHING_THREAD_POOL_SIZE,
                        new ExecutorThreadFactory(
                                "http-sink-client-response-worker",
                                ThreadUtils.LOGGING_EXCEPTION_HANDLER));

        this.httpRequestTimeOutSeconds =
                Integer.parseInt(
                        properties.getProperty(
                                HttpConnectorConfigConstants.SINK_HTTP_TIMEOUT_SECONDS,
                                DEFAULT_REQUEST_TIMEOUT_SECONDS));

        this.httpClient = httpClient;
    }
}
