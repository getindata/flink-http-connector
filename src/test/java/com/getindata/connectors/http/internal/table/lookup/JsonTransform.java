package com.getindata.connectors.http.internal.table.lookup;

import java.util.concurrent.atomic.AtomicInteger;

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
public class JsonTransform extends ResponseTransformer {

    public static final String NAME = "JsonTransform";

    private static final String RESULT_JSON =
        "{\n"
            + "\t\"id\": \"&COUNTER&\",\n"
            + "\t\"id2\": \"&COUNTER_2&\",\n"
            + "\t\"uuid\": \"fbb68a46-80a9-46da-9d40-314b5287079c\",\n"
            + "\t\"picture\": \"http://placehold.it/32x32\",\n"
            + "\t\"msg\": \"&PARAM&, cnt: &COUNTER&\",\n"
            + "\t\"age\": 30,\n"
            + "\t\"eyeColor\": \"green\",\n"
            + "\t\"name\": \"Marva Fischer\",\n"
            + "\t\"gender\": \"female\",\n"
            + "\t\"company\": \"SILODYNE\",\n"
            + "\t\"email\": \"marvafischer@silodyne.com\",\n"
            + "\t\"phone\": \"+1 (990) 562-2120\",\n"
            + "\t\"address\": \"601 Auburn Place, Bynum, New York, 7057\",\n"
            + "\t\"about\": \"Proident Lorem et duis nisi tempor elit occaecat laboris"
            + " dolore magna Lorem consequat. Deserunt velit minim nisi consectetur duis "
            + "amet labore cupidatat. Pariatur sunt occaecat qui reprehenderit ipsum ex culpa "
            + "ullamco ex duis adipisicing commodo sunt. Ad cupidatat magna ad in officia "
            + "irure aute duis culpa et. Magna esse adipisicing consequat occaecat. Excepteur amet "
            + "dolore occaecat sit officia dolore elit in cupidatat non anim.\\r\\n\",\n"
            + "\t\"registered\": \"2020-07-11T11:13:32 -02:00\",\n"
            + "\t\"latitude\": -35.237843,\n"
            + "\t\"longitude\": 60.386104,\n"
            + "\t\"tags\": [\n"
            + "\t\t\"officia\",\n"
            + "\t\t\"eiusmod\",\n"
            + "\t\t\"labore\",\n"
            + "\t\t\"ex\",\n"
            + "\t\t\"aliqua\",\n"
            + "\t\t\"consectetur\",\n"
            + "\t\t\"excepteur\"\n"
            + "\t],\n"
            + "\t\"friends\": [\n"
            + "\t\t{\n"
            + "\t\t\t\"id\": 0,\n"
            + "\t\t\t\"name\": \"Kemp Newman\"\n"
            + "\t\t},\n"
            + "\t\t{\n"
            + "\t\t\t\"id\": 1,\n"
            + "\t\t\t\"name\": \"Sears Blackburn\"\n"
            + "\t\t},\n"
            + "\t\t{\n"
            + "\t\t\t\"id\": 2,\n"
            + "\t\t\t\"name\": \"Lula Rogers\"\n"
            + "\t\t}\n"
            + "\t],\n"
            + "\t\"details\": {\n"
            + "\t\t\"isActive\": true,\n"
            + "\t\t\"nestedDetails\": {\n"
            + "\t\t\t\"index\": 0,\n"
            + "\t\t\t\"guid\": \"d81fc542-6b49-4d59-8fb9-d57430d4871d\",\n"
            + "\t\t\t\"balance\": \"$1,729.34\"\n"
            + "\t\t}\n"
            + "\t},\n"
            + "\t\"greeting\": \"Hello, Marva Fischer! You have 7 unread messages.\",\n"
            + "\t\"favoriteFruit\": \"banana\"\n"
            + "}";
    private final AtomicInteger counter = new AtomicInteger(0);

    @Override
    public Response transform(
        Request request, Response response, FileSource files, Parameters parameters) {
        int cnt = counter.getAndIncrement();

        return Response.response()
            .body(generateResponse(request.getUrl(), cnt))
            .status(response.getStatus())
            .statusMessage(response.getStatusMessage())
            .build();
    }

    @Override
    public String getName() {
        return NAME;
    }

    private String generateResponse(String param, int counter) {
        return RESULT_JSON
            .replaceAll("&PARAM&", param)
            .replaceAll("&COUNTER&", String.valueOf(counter))
            .replaceAll("&COUNTER_2&", String.valueOf(counter + 1));
    }
}
