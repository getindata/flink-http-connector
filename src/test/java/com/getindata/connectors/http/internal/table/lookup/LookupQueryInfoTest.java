package com.getindata.connectors.http.internal.table.lookup;

import java.util.Map;

import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;

class LookupQueryInfoTest {

    private LookupQueryInfo lookupQueryInfo;

    @Test
    public void testConfiguredLookupQuery() {
        String lookupQuery = "{\"param1\": \"value1\"}";
        Map<String, String> bodyBasedUrlQueryParameters = Map.of("key1", "value1");

        lookupQueryInfo = new LookupQueryInfo(lookupQuery, bodyBasedUrlQueryParameters);

        assertThat(lookupQueryInfo.hasLookupQuery()).isTrue();
        assertThat(lookupQueryInfo.getLookupQuery()).isEqualTo(lookupQuery);
        assertThat(lookupQueryInfo.hasBodyBasedUrlQueryParameters()).isTrue();
        assertThat(lookupQueryInfo.getBodyBasedUrlQueryParameters()).isEqualTo("key1=value1");
    }
    @Test
    public void testEmptyLookupQueryInfo() {
        lookupQueryInfo = new LookupQueryInfo(null, null);

        assertThat(lookupQueryInfo.hasLookupQuery()).isFalse();
        assertThat(lookupQueryInfo.hasBodyBasedUrlQueryParameters()).isFalse();
        assertThat(lookupQueryInfo.getLookupQuery()).isEqualTo("");
        assertThat(lookupQueryInfo.getBodyBasedUrlQueryParameters()).isEqualTo("");
    }

    @Test
    public void testBodyBasedUrlQueryParams() {
        Map<String, String> bodyBasedUrlQueryParameters = Map.of("key1", "value1");

        lookupQueryInfo = new LookupQueryInfo(null, bodyBasedUrlQueryParameters);

        assertThat(lookupQueryInfo.hasLookupQuery()).isFalse();
        assertThat(lookupQueryInfo.hasBodyBasedUrlQueryParameters()).isTrue();
        assertThat(lookupQueryInfo.getBodyBasedUrlQueryParameters()).isEqualTo("key1=value1");
    }
}
