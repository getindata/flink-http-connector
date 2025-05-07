package com.getindata.connectors.http.internal.table.lookup.querycreators;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.MapperFeature;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.SerializationFeature;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.json.JsonMapper;

/**
 * Centralizes the use of {@link ObjectMapper}.
 */
public class ObjectMapperAdapter {
    private static final ObjectMapper MAPPER = initialize();

    private static ObjectMapper initialize() {
        final ObjectMapper mapper = JsonMapper.builder()
                .configure(MapperFeature.USE_STD_BEAN_NAMING,
                        false).build();
        mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        mapper.disable(SerializationFeature.WRITE_DATES_WITH_ZONE_ID);
        mapper.disable(DeserializationFeature.ADJUST_DATES_TO_CONTEXT_TIME_ZONE);
        return mapper;
    }

    public static ObjectMapper instance() {
        return MAPPER;
    }
}
