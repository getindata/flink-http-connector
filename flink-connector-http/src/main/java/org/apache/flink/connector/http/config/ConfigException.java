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

package org.apache.flink.connector.http.config;

/**
 * A Runtime exception throw when there is any issue with configuration properties for Http
 * Connector.
 */
public class ConfigException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public ConfigException(String message) {
        super(message);
    }

    public ConfigException(String message, Throwable t) {
        super(message, t);
    }

    /**
     * Creates an exception object using predefined exception message template: {@code Invalid value
     * + (value) + for configuration + (property name) + (additional message) }.
     *
     * @param name configuration property name.
     * @param value configuration property value.
     * @param message custom message appended to the end of exception message.
     */
    public ConfigException(String name, Object value, String message) {
        super(
                "Invalid value "
                        + value
                        + " for configuration "
                        + name
                        + (message == null ? "" : ": " + message));
    }
}
