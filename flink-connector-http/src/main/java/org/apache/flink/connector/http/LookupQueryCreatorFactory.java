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

package org.apache.flink.connector.http;

import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.http.table.lookup.HttpLookupTableSource;
import org.apache.flink.connector.http.table.lookup.LookupRow;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.Factory;

import java.io.Serializable;

/**
 * The {@link Factory} that dynamically creates and injects {@link LookupQueryCreator} to {@link
 * HttpLookupTableSource}.
 *
 * <p>Custom implementations of {@link LookupQueryCreatorFactory} can be registered along other
 * factories in
 *
 * <pre>resources/META-INF.services/org.apache.flink.table.factories.Factory</pre>
 *
 * <p>file and then referenced by their identifiers in the HttpLookupSource DDL property field
 * <i>flink.connector.http.source.lookup.query-creator</i>.
 *
 * <p>The following example shows the minimum Table API example to create a {@link
 * HttpLookupTableSource} that uses a custom query creator created by a factory that returns
 * <i>my-query-creator</i> as its identifier.
 *
 * <pre>{@code
 * CREATE TABLE http (
 *   id bigint,
 *   some_field string
 * ) WITH (
 *   'connector' = 'rest-lookup',
 *   'format' = 'json',
 *   'url' = 'http://example.com/myendpoint',
 *   'http.source.lookup.query-creator' = 'my-query-creator'
 * )
 * }</pre>
 */
public interface LookupQueryCreatorFactory extends Factory, Serializable {

    /**
     * @param readableConfig readable config
     * @param lookupRow lookup row
     * @param dynamicTableFactoryContext context
     * @return {@link LookupQueryCreator} custom lookup query creator instance
     */
    LookupQueryCreator createLookupQueryCreator(
            ReadableConfig readableConfig,
            LookupRow lookupRow,
            DynamicTableFactory.Context dynamicTableFactoryContext);
}
