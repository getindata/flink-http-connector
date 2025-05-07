/*
 * © Copyright IBM Corp. 2025
 */

package com.getindata.connectors.http.internal.table.lookup.querycreators;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.*;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.table.api.DataTypes.Field;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.FieldsDataType;
import org.apache.flink.types.Row;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.Preconditions;

import com.getindata.connectors.http.LookupArg;
import com.getindata.connectors.http.LookupQueryCreator;
import com.getindata.connectors.http.internal.table.lookup.LookupQueryInfo;
import com.getindata.connectors.http.internal.table.lookup.LookupRow;
import com.getindata.connectors.http.internal.utils.SerializationSchemaUtils;

/**
 * Generic JSON and URL query creator; in addition to be able to map columns to json requests,
 * it allows url inserts to be mapped to column names using templating.
 * <br>
 * For GETs, column names are mapped to query parameters. e.g. for
 * <code>GenericJsonAndUrlQueryCreator.REQUEST_PARAM_FIELDS</code> = "id1;id2"
 * and url of http://base. At lookup time with values of id1=1 and id2=2 a call of
 * http/base?id1=1&amp;id2=2 will be issued.
 * <br>
 * For PUT and POST, parameters are mapped to the json body  e.g. for
 * REQUEST_PARAM_FIELDS = "id1;id2" and url of http://base. At lookup time with values of id1=1 and
 * id2=2 as call of http/base will be issued with a json payload of {"id1":1,"id2":2}
 * <br>
 * For all http methods, url segments can be used to include lookup up values. Using the map from
 * <code>GenericJsonAndUrlQueryCreator.REQUEST_URL_MAP</code> which has a key of the insert and the
 * value of the associated column.
 * e.g. for <code>GenericJsonAndUrlQueryCreator.REQUEST_URL_MAP</code> = "key1":"col1"
 * and url of http://base/{key1}. At lookup time with values of col1="aaaa" a call of
 * http/base/aaaa will be issued.
 *
 */
@Slf4j
public class GenericJsonAndUrlQueryCreator implements LookupQueryCreator {
    private static final long serialVersionUID = 1L;

    // not final so we can mutate for unit test
    private SerializationSchema<RowData> serializationSchema;
    private boolean schemaOpened = false;
    private LookupRow lookupRow;
    private final String httpMethod;
    private final List<String>  requestQueryParamsFields;
    private final List<String> requestBodyFields;
    private final Map<String, String> requestUrlMap;

    /**
     * Construct a Generic JSON and URL query creator.
     *
     * @param httpMethod the requested http method
     * @param serializationSchema    serialization schema for RowData
     * @param requestQueryParamsFields query param fields
     * @param requestBodyFields body fields used for PUT and POSTs
     * @param requestUrlMap url map
     * @param lookupRow     lookup row itself.
     */
    public GenericJsonAndUrlQueryCreator(final String httpMethod,
                                         final SerializationSchema<RowData>
                                             serializationSchema,
                                         final List<String>  requestQueryParamsFields,
                                         final List<String> requestBodyFields,
                                         final Map<String, String> requestUrlMap,
                                         final LookupRow lookupRow) {
        this.httpMethod = httpMethod;
        this.serializationSchema = serializationSchema;
        this.lookupRow = lookupRow;
        this.requestQueryParamsFields = requestQueryParamsFields;
        this.requestBodyFields = requestBodyFields;
        this.requestUrlMap = requestUrlMap;
    }
    @VisibleForTesting
    void setSerializationSchema(SerializationSchema<RowData>
                                        serializationSchema) {
        this.serializationSchema = serializationSchema;
    }

    @Override
    public LookupQueryInfo createLookupQuery(final RowData lookupDataRow) {
        this.checkOpened();

        final String lookupQuery;
        Map<String, String> bodyBasedUrlQueryParams = new HashMap<>();
        final Collection<LookupArg> lookupArgs =
                lookupRow.convertToLookupArgs(lookupDataRow);
        ObjectNode jsonObject;
        try {
            jsonObject = (ObjectNode) ObjectMapperAdapter.instance().readTree(
                    serializationSchema.serialize(lookupDataRow));
        } catch (IOException e) {
            String message = "Unable to parse the lookup arguments to json.";
            log.error(message, e);
            throw new RuntimeException(message, e);
        }
        // Parameters are encoded as query params for GET and none GET.
        // Later code will turn these query params into the body for PUTs and POSTs
        ObjectNode jsonObjectForQueryParams = ObjectMapperAdapter.instance().createObjectNode();
        for (String requestColumnName : this.requestQueryParamsFields) {
            jsonObjectForQueryParams.set(requestColumnName, jsonObject.get(requestColumnName));
        }
        // TODO can we convertToQueryParameters for all ops
        //  and not use/deprecate bodyBasedUrlQueryParams
        if (httpMethod.equalsIgnoreCase("GET")) {
            // add the query parameters
            lookupQuery = convertToQueryParameters(jsonObjectForQueryParams,
                    StandardCharsets.UTF_8.toString());
        } else {
            // Body-based queries
            // serialize to a string for the body.
            try {
                lookupQuery = ObjectMapperAdapter.instance()
                        .writeValueAsString(jsonObject.retain(requestBodyFields));
            } catch (JsonProcessingException e) {
                final String message = "Unable to convert Json Object to a string";
                throw new RuntimeException(message,e);
            }
            // body parameters
            // use the request json object to scope the required fields and the lookupArgs as values
            bodyBasedUrlQueryParams = createBodyBasedParams(lookupArgs,
                    jsonObjectForQueryParams);
        }
        // add the path map
        final Map<String, String> pathBasedUrlParams = createPathBasedParams(lookupArgs,
                requestUrlMap);

        return new LookupQueryInfo(lookupQuery, bodyBasedUrlQueryParams, pathBasedUrlParams);

    }

    /**
     * Create a Row from a RowData and DataType
     * @param lookupRowData the lookup RowData
     * @param rowType the datatype
     * @return row return row
     */
    @VisibleForTesting
    static Row rowDataToRow(final RowData lookupRowData, final DataType rowType) {
        Preconditions.checkNotNull(lookupRowData);
        Preconditions.checkNotNull(rowType);

        final Row row = Row.withNames();
        final List<Field> rowFields = FieldsDataType.getFields(rowType);

        for (int idx = 0; idx < rowFields.size(); idx++) {
            final String fieldName = rowFields.get(idx).getName();
            final Object fieldValue = ((GenericRowData) lookupRowData).getField(idx);
            row.setField(fieldName, fieldValue);
        }
        return row;
    }

    /**
     * Create map of the json key to the lookup argument
     * value. This is used for body based content.
     * @param args lookup arguments
     * @param objectNode object node
     * @return map of field content to the lookup argument value.
     */
    private Map<String, String> createBodyBasedParams(final Collection<LookupArg> args,
                                                              ObjectNode objectNode ) {
        Map<String, String> mapOfJsonKeyToLookupArg = new LinkedHashMap<>();
        Iterator<Map.Entry<String, JsonNode>> iterator = objectNode.fields();
        iterator.forEachRemaining(field -> {
            for (final LookupArg arg : args) {
                if (arg.getArgName().equals(field.getKey())) {
                    String keyForMap = field.getKey();
                    mapOfJsonKeyToLookupArg.put(
                            keyForMap, arg.getArgValue());
                }
            }
        });

        return mapOfJsonKeyToLookupArg;
    }
    /**
     * Create map of the json key to the lookup argument
     * value. This is used for path based content.
     * @param args lookup arguments
     * @param urlMap map of insert name to column name
     * @return map of field content to the lookup argument value.
     */
    private Map<String, String> createPathBasedParams(final Collection<LookupArg> args,
                                                     Map<String, String> urlMap ) {
        Map<String, String> mapOfJsonKeyToLookupArg = new LinkedHashMap<>();
        if (urlMap != null) {
            for (String key: urlMap.keySet()) {
                for (final LookupArg arg : args) {
                    if (arg.getArgName().equals(key)) {
                        mapOfJsonKeyToLookupArg.put(
                                urlMap.get(key), arg.getArgValue());
                    }
                }
            }
        }
        return mapOfJsonKeyToLookupArg;
    }
    /**
     * Convert json object to query params string
     * @param jsonObject supplies json object
     * @param enc encoding string - used in unit test to drive unsupported encoding
     * @return query params string
     */
    @VisibleForTesting
    static String convertToQueryParameters(final ObjectNode jsonObject, String enc) {
        Preconditions.checkNotNull(jsonObject);

        final StringJoiner result = new StringJoiner("&");
        jsonObject.fields().forEachRemaining(field -> {
            final String fieldName = field.getKey();
            final String fieldValue = field.getValue().asText();

            try {
                result.add(fieldName + "="
                        + URLEncoder.encode(fieldValue, enc));
            } catch (UnsupportedEncodingException e) {
                final String message =
                        "Failed to encode the value of the query parameter name "
                                + fieldName
                                + ": "
                                + fieldValue;
                throw new RuntimeException(message, e);
            }
        });

        return result.toString();
    }

    private void checkOpened() {
        if (!this.schemaOpened) {
            try {
                this.serializationSchema.open(
                        SerializationSchemaUtils
                                .createSerializationInitContext(
                                        GenericJsonAndUrlQueryCreator.class));
                this.schemaOpened = true;
            } catch (final Exception e) {
                final String message =
                        "Failed to initialize serialization schema for "
                                + GenericJsonAndUrlQueryCreator.class;
                log.error(message, e);
                throw new FlinkRuntimeException(message, e);
            }
        }
    }
}
