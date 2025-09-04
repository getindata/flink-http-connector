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

package org.apache.flink.connector.http.table.lookup.querycreators;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.http.LookupQueryCreator;
import org.apache.flink.connector.http.LookupQueryCreatorFactory;
import org.apache.flink.connector.http.table.lookup.LookupRow;
import org.apache.flink.table.factories.DynamicTableFactory;

import java.util.Set;

/** Factory for creating {@link GenericGetQueryCreator}. */
public class GenericGetQueryCreatorFactory implements LookupQueryCreatorFactory {

    public static final String IDENTIFIER = "generic-get-query";

    @Override
    public LookupQueryCreator createLookupQueryCreator(
            ReadableConfig readableConfig,
            LookupRow lookupRow,
            DynamicTableFactory.Context dynamicTableFactoryContext) {
        return new GenericGetQueryCreator(lookupRow);
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return Set.of();
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return Set.of();
    }
}
