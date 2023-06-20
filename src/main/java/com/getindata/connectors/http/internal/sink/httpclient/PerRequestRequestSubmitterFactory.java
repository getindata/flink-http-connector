package com.getindata.connectors.http.internal.sink.httpclient;

import java.util.Properties;

public class PerRequestRequestSubmitterFactory implements RequestSubmitterFactory {

    @Override
    public RequestSubmitter createSubmitter(Properties properties, String[] headersAndValues) {
        return new PerRequestSubmitter(properties, headersAndValues);
    }
}
