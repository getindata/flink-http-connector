package com.getindata.connectors.http.internal.table.lookup;

import com.github.tomakehurst.wiremock.common.FileSource;
import com.github.tomakehurst.wiremock.extension.Parameters;
import com.github.tomakehurst.wiremock.extension.ResponseTransformer;
import com.github.tomakehurst.wiremock.http.Request;
import com.github.tomakehurst.wiremock.http.Response;

/**
 * Wiremock Extension that prepares HTTP REST endpoint response body. This extension returns a status code 500 error
 * response.
 */
public class JsonTransformBadstatus extends ResponseTransformer {

    public static final String NAME = "JsonTransformBadStatus";

    @Override
    public Response transform(
        Request request, Response response, FileSource files, Parameters parameters) {
        return Response.response()
            .body("Response body for 500")
            .status(500)
            .statusMessage("Test status message with 500")
            .build();
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
