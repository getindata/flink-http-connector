package com.getindata.connectors.http.internal.table.lookup;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.table.data.RowData;

import com.getindata.connectors.http.internal.utils.SerializationSchemaUtils;

@Slf4j
@AllArgsConstructor
public class RawResponseBodyDecoder implements Serializable {

    private final DeserializationSchema<RowData> deserializationSchema;

    public void open() throws Exception {
        this.deserializationSchema.open(
            SerializationSchemaUtils
                .createDeserializationInitContext(RawResponseBodyDecoder.class));
    }

    public Collection<RowData> deserialize(Collection<byte[]> rawItems) {
        List<RowData> result = new ArrayList<>(rawItems.size());

        for (byte[] rawItem : rawItems) {
            try {
                result.add(deserializationSchema.deserialize(rawItem));
            } catch (IOException e) {
                log.error("Exception during object deserialization.", e);
            }
        }
        return result;
    }

}
