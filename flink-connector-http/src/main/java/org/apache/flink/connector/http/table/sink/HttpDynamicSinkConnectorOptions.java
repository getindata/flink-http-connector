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
import org.apache.flink.configuration.ConfigOptions;

import static org.apache.flink.connector.http.config.HttpConnectorConfigConstants.SINK_REQUEST_CALLBACK_IDENTIFIER;

/** Table API options for {@link HttpDynamicSink}. */
public class HttpDynamicSinkConnectorOptions {

    public static final ConfigOption<String> URL =
            ConfigOptions.key("url")
                    .stringType()
                    .noDefaultValue()
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
}
