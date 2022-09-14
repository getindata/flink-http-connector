package com.getindata.connectors.http.internal.table.lookup;

import java.net.http.HttpClient;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.table.data.RowData;

import com.getindata.connectors.http.internal.HeaderPreprocessor;
import com.getindata.connectors.http.internal.PollingClientFactory;
import com.getindata.connectors.http.internal.utils.HttpHeaderUtils;
import com.getindata.connectors.http.internal.utils.JavaNetHttpClientFactory;

public class JavaNetHttpPollingClientFactory implements PollingClientFactory<RowData> {

    @Override
    public JavaNetHttpPollingClient createPollClient(
            HttpLookupConfig options,
            DeserializationSchema<RowData> schemaDecoder) {

        HttpClient httpClient = JavaNetHttpClientFactory.createClient(options.getProperties());

        // TODO Consider this to be injected as method argument or factory field
        //  so user could set this using API.
        HeaderPreprocessor headerPreprocessor = HttpHeaderUtils.createDefaultHeaderPreprocessor();


        return new JavaNetHttpPollingClient(httpClient, schemaDecoder, options, headerPreprocessor);
    }
}
