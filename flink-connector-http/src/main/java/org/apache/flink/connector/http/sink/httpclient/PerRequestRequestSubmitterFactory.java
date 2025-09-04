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

import org.apache.flink.connector.http.utils.JavaNetHttpClientFactory;
import org.apache.flink.connector.http.utils.ThreadUtils;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;

import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/** Per request submitter factory. */
public class PerRequestRequestSubmitterFactory implements RequestSubmitterFactory {

    // TODO Add this property to config. Make sure to add note in README.md that will describe that
    //  any value greater than one will break order of messages.
    static final int HTTP_CLIENT_THREAD_POOL_SIZE = 1;

    @Override
    public RequestSubmitter createSubmitter(Properties properties, String[] headersAndValues) {

        ExecutorService httpClientExecutor =
                Executors.newFixedThreadPool(
                        HTTP_CLIENT_THREAD_POOL_SIZE,
                        new ExecutorThreadFactory(
                                "http-sink-client-per-request-worker",
                                ThreadUtils.LOGGING_EXCEPTION_HANDLER));

        return new PerRequestSubmitter(
                properties,
                headersAndValues,
                JavaNetHttpClientFactory.createClient(properties, httpClientExecutor));
    }
}
