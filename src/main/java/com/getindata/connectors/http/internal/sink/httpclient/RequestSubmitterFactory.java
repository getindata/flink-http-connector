package com.getindata.connectors.http.internal.sink.httpclient;

import java.util.Properties;

public interface RequestSubmitterFactory {

    RequestSubmitter createSubmitter(Properties properties, String[] headersAndValues);
}
