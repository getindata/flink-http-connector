package com.getindata.connectors.http;

import org.apache.flink.api.common.serialization.SerializationSchema.InitializationContext;
import org.apache.flink.api.connector.sink2.Sink.InitContext;
import org.apache.flink.connector.base.sink.writer.ElementConverter;

/**
 * An enhancement for Flink's {@link ElementConverter} that expose {@link #open(InitContext)} method
 * that will be called by HTTP connect code to ensure that element converter is initialized
 * properly. This is required for cases when Flink's SerializationSchema and DeserializationSchema
 * objects like JsonRowDataSerializationSchema are used.
 * <p>
 * This interface specifies the mapping between elements of a stream to request entries that can be
 * sent to the destination. The mapping is provided by the end-user of a sink, not the sink
 * creator.
 *
 * <p>The request entries contain all relevant information required to create and sent the actual
 * request. Eg, for Kinesis Data Streams, the request entry includes the payload and the partition
 * key.
 */
public interface SchemaLifecycleAwareElementConverter<InputT, RequestEntryT>
    extends ElementConverter<InputT, RequestEntryT> {

    /**
     * Initialization element converter for the schema.
     *
     * <p>The provided {@link InitializationContext} can be used to access additional features such
     * as e.g. registering user metrics.
     *
     * @param context Contextual information that can be used during initialization.
     */
    void open(InitContext context);

}
