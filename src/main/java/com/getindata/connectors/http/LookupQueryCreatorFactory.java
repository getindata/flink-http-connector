package com.getindata.connectors.http;

import java.io.Serializable;

import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.Factory;

import com.getindata.connectors.http.internal.table.lookup.HttpLookupTableSource;
import com.getindata.connectors.http.internal.table.lookup.LookupRow;

/**
 * The {@link Factory} that dynamically creates and injects {@link LookupQueryCreator} to
 * {@link HttpLookupTableSource}.
 *
 * <p>Custom implementations of {@link LookupQueryCreatorFactory} can be registered along other
 * factories in <pre>resources/META-INF.services/org.apache.flink.table.factories.Factory</pre>
 * file and then referenced by their identifiers in the HttpLookupSource DDL property field
 * <i>gid.connector.http.source.lookup.query-creator</i>.
 *
 * <p>The following example shows the minimum Table API example to create a
 * {@link HttpLookupTableSource} that uses a custom query creator created by a factory that returns
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
 *   'gid.connector.http.source.lookup.query-creator' = 'my-query-creator'
 * )
 * }</pre>
 */
public interface LookupQueryCreatorFactory extends Factory, Serializable {

    /**
     * @return {@link LookupQueryCreator} custom lookup query creator instance
     */
    LookupQueryCreator createLookupQueryCreator(
        ReadableConfig readableConfig,
        LookupRow lookupRow,
        DynamicTableFactory.Context dynamicTableFactoryContext);
}
