/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.http.config;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link ConfigException}. */
class ConfigExceptionTest {

    @Test
    public void testTemplateMessageWithNull() {
        ConfigException exception = new ConfigException("myProp", -1, null);
        assertThat(exception.getMessage()).isEqualTo("Invalid value -1 for configuration myProp");
    }

    @Test
    public void testTemplateMessage() {
        ConfigException exception = new ConfigException("myProp", -1, "Invalid test value.");
        assertThat(exception.getMessage())
                .isEqualTo("Invalid value -1 for configuration myProp: Invalid test value.");
    }
}
