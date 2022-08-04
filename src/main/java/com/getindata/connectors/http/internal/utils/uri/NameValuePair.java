package com.getindata.connectors.http.internal.utils.uri;

import lombok.Data;
import org.apache.flink.util.Preconditions;

@Data
class NameValuePair {

    private final String name;

    private final String value;

    /**
     * Default Constructor taking a name and a value. The value may be null.
     *
     * @param name The name.
     * @param value The value.
     */
    NameValuePair(final String name, final String value) {
        super();
        this.name = Preconditions.checkNotNull(name, "Name may not be null");
        this.value = value;
    }
}
