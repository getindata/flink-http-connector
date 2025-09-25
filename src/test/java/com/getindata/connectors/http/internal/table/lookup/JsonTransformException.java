package com.getindata.connectors.http.internal.table.lookup;

import com.github.tomakehurst.wiremock.common.FileSource;
import com.github.tomakehurst.wiremock.extension.Parameters;
import com.github.tomakehurst.wiremock.extension.ResponseTransformer;
import com.github.tomakehurst.wiremock.http.Request;
import com.github.tomakehurst.wiremock.http.Response;

/**
 * Wiremock Extension that prepares HTTP REST endpoint response body. This extension is stateful,
 * every next response will have id == counter and id2 == counter + 1 value in its response, where
 * counter is incremented for every subsequent request.
 */
public class JsonTransformException extends ResponseTransformer {

    public static final String NAME = "JsonTransformException";

    @Override
    public Response transform(
        Request request, Response response, FileSource files, Parameters parameters) {
        throw new RuntimeException("Test runtime exception");
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public boolean applyGlobally() {
        return false;
    }
}
