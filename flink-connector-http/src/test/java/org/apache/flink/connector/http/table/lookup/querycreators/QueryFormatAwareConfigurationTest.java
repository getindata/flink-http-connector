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

package org.apache.flink.connector.http.table.lookup.querycreators;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link QueryFormatAwareConfiguration }. */
class QueryFormatAwareConfigurationTest {

    private static final ConfigOption<String> configOption =
            ConfigOptions.key("key").stringType().noDefaultValue();

    @Test
    public void testWithDot() {
        QueryFormatAwareConfiguration queryConfig =
                new QueryFormatAwareConfiguration(
                        "prefix.",
                        Configuration.fromMap(Collections.singletonMap("prefix.key", "val")));

        Optional<String> optional = queryConfig.getOptional(configOption);
        assertThat(optional.get()).isEqualTo("val");
    }

    @Test
    public void testWithoutDot() {
        QueryFormatAwareConfiguration queryConfig =
                new QueryFormatAwareConfiguration(
                        "prefix",
                        Configuration.fromMap(Collections.singletonMap("prefix.key", "val")));

        Optional<String> optional = queryConfig.getOptional(configOption);
        assertThat(optional.get()).isEqualTo("val");
    }
}
