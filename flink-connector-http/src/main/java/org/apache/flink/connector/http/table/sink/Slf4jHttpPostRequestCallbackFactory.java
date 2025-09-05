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
import org.apache.flink.connector.http.HttpPostRequestCallback;
import org.apache.flink.connector.http.HttpPostRequestCallbackFactory;
import org.apache.flink.connector.http.sink.httpclient.HttpRequest;

import java.util.HashSet;
import java.util.Set;

/** Factory for creating {@link Slf4jHttpPostRequestCallback}. */
public class Slf4jHttpPostRequestCallbackFactory
        implements HttpPostRequestCallbackFactory<HttpRequest> {

    public static final String IDENTIFIER = "slf4j-logger";

    @Override
    public HttpPostRequestCallback<HttpRequest> createHttpPostRequestCallback() {
        return new Slf4jHttpPostRequestCallback();
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return new HashSet<>();
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return new HashSet<>();
    }
}
