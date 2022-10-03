package com.getindata.connectors.http.internal.table.lookup.querycreators;

import java.util.List;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

import com.getindata.connectors.http.LookupArg;
import com.getindata.connectors.http.LookupQueryCreator;

/**
 * A {@link LookupQueryCreator} that builds Json based body for REST requests, i.e. adds
 */
public class GenericJsonQueryCreator implements LookupQueryCreator {

    private final ObjectMapper objectMapper;

    public GenericJsonQueryCreator() {
        this.objectMapper = new ObjectMapper();
    }

    /**
     * Creates a Jason string from given List of {@link LookupArg}.
     * The list is converted to a Json objects containing pairs of {@code LookupArg.getArgName()}
     * and {@code LookupArg.getArgValue()} from params parameter.
     * <p>
     * A two element array of {@link LookupArg} objects, were first element contains a pair of
     * {@code "id" -> "1"} and second element contains a pair of {@code "uuid" -> "2"}
     * will be converted to below Json:
     * <pre>
     *     {
     *         "id": "1",
     *         "uuid": "2"
     *     }
     * </pre>
     *
     * @param params the list of {@link LookupArg} containing request parameters that will be
     *               converted to Json string.
     * @return Json string created from param argument.
     */
    @Override
    public String createLookupQuery(List<LookupArg> params) {
        ObjectNode root = this.objectMapper.createObjectNode();
        for (LookupArg arg : params) {
            root.put(arg.getArgName(), arg.getArgValue());
        }
        return root.toString();
    }
}
