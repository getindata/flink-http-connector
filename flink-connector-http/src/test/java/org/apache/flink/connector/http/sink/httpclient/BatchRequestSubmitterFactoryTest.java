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

package org.apache.flink.connector.http.sink.httpclient;

import org.apache.flink.connector.http.config.ConfigException;
import org.apache.flink.connector.http.config.HttpConnectorConfigConstants;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Test for {@link BatchRequestSubmitterFactory}. */
class BatchRequestSubmitterFactoryTest {

    @ParameterizedTest
    @ValueSource(ints = {0, -1})
    public void shouldThrowIfInvalidDefaultSize(int invalidArgument) {
        assertThrows(
                IllegalArgumentException.class,
                () -> new BatchRequestSubmitterFactory(invalidArgument));
    }

    @Test
    public void shouldCreateSubmitterWithDefaultBatchSize() {

        int defaultBatchSize = 10;
        BatchRequestSubmitter submitter =
                new BatchRequestSubmitterFactory(defaultBatchSize)
                        .createSubmitter(new Properties(), new String[0]);

        assertThat(submitter.getBatchSize()).isEqualTo(defaultBatchSize);
    }

    @ParameterizedTest
    @ValueSource(strings = {"1", "2"})
    public void shouldCreateSubmitterWithCustomBatchSize(String batchSize) {

        Properties properties = new Properties();
        properties.setProperty(
                HttpConnectorConfigConstants.SINK_HTTP_BATCH_REQUEST_SIZE, batchSize);

        BatchRequestSubmitter submitter =
                new BatchRequestSubmitterFactory(10).createSubmitter(properties, new String[0]);

        assertThat(submitter.getBatchSize()).isEqualTo(Integer.valueOf(batchSize));
    }

    @ParameterizedTest
    @ValueSource(strings = {"0", "-1"})
    public void shouldThrowIfBatchSizeToSmall(String invalidBatchSize) {

        Properties properties = new Properties();
        properties.setProperty(
                HttpConnectorConfigConstants.SINK_HTTP_BATCH_REQUEST_SIZE, invalidBatchSize);

        BatchRequestSubmitterFactory factory = new BatchRequestSubmitterFactory(10);

        assertThrows(
                ConfigException.class, () -> factory.createSubmitter(properties, new String[0]));
    }

    @ParameterizedTest
    @ValueSource(strings = {"1.1", "2,2", "hello"})
    public void shouldThrowIfInvalidBatchSize(String invalidBatchSize) {

        Properties properties = new Properties();
        properties.setProperty(
                HttpConnectorConfigConstants.SINK_HTTP_BATCH_REQUEST_SIZE, invalidBatchSize);

        BatchRequestSubmitterFactory factory = new BatchRequestSubmitterFactory(10);

        assertThrows(
                ConfigException.class, () -> factory.createSubmitter(properties, new String[0]));
    }
}
