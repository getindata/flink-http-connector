package com.getindata.connectors.http.internal.table.lookup;

import java.util.ArrayList;
import java.util.List;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.DataTypes.Field;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.AsyncTableFunctionProvider;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.connector.source.TableFunctionProvider;
import org.apache.flink.table.connector.source.abilities.SupportsLimitPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsProjectionPushDown;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.RowType.RowField;
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks;

import com.getindata.connectors.http.LookupQueryCreator;
import com.getindata.connectors.http.LookupQueryCreatorFactory;
import com.getindata.connectors.http.internal.HeaderPreprocessor;
import com.getindata.connectors.http.internal.PollingClientFactory;
import com.getindata.connectors.http.internal.table.lookup.querycreators.GenericGetQueryCreatorFactory;
import com.getindata.connectors.http.internal.table.lookup.querycreators.GenericJsonQueryCreatorFactory;
import com.getindata.connectors.http.internal.utils.HttpHeaderUtils;
import static com.getindata.connectors.http.internal.table.lookup.HttpLookupConnectorOptions.LOOKUP_QUERY_CREATOR_IDENTIFIER;
import static com.getindata.connectors.http.internal.table.lookup.HttpLookupTableSourceFactory.row;

@Slf4j
public class HttpLookupTableSource
    implements LookupTableSource, SupportsProjectionPushDown, SupportsLimitPushDown {

    private final DataType physicalRowDataType;

    private final HttpLookupConfig lookupConfig;

    private final DynamicTableFactory.Context dynamicTableFactoryContext;

    private final DecodingFormat<DeserializationSchema<RowData>> decodingFormat;

    public HttpLookupTableSource(
            DataType physicalRowDataType,
            HttpLookupConfig lookupConfig,
            DecodingFormat<DeserializationSchema<RowData>> decodingFormat,
            DynamicTableFactory.Context dynamicTablecontext) {

        this.physicalRowDataType = physicalRowDataType;
        this.lookupConfig = lookupConfig;
        this.decodingFormat = decodingFormat;
        this.dynamicTableFactoryContext = dynamicTablecontext;
    }

    @Override
    public LookupRuntimeProvider getLookupRuntimeProvider(LookupContext lookupContext) {

        LookupRow lookupRow = extractLookupRow(lookupContext.getKeys());

        DeserializationSchema<RowData> responseSchemaDecoder =
            decodingFormat.createRuntimeDecoder(lookupContext, physicalRowDataType);

        LookupQueryCreatorFactory lookupQueryCreatorFactory =
            FactoryUtil.discoverFactory(
                this.dynamicTableFactoryContext.getClassLoader(),
                LookupQueryCreatorFactory.class,
                lookupConfig.getReadableConfig().getOptional(LOOKUP_QUERY_CREATOR_IDENTIFIER)
                    .orElse(
                        (lookupConfig.getLookupMethod().equalsIgnoreCase("GET") ?
                            GenericGetQueryCreatorFactory.IDENTIFIER :
                            GenericJsonQueryCreatorFactory.IDENTIFIER)
                    )
            );
        ReadableConfig readableConfig = lookupConfig.getReadableConfig();
        LookupQueryCreator lookupQueryCreator =
            lookupQueryCreatorFactory.createLookupQueryCreator(
                readableConfig,
                lookupRow,
                dynamicTableFactoryContext
            );

        PollingClientFactory<RowData> pollingClientFactory =
            createPollingClientFactory(lookupQueryCreator, lookupConfig);

        HttpTableLookupFunction dataLookupFunction =
            new HttpTableLookupFunction(
                pollingClientFactory,
                responseSchemaDecoder,
                lookupRow,
                lookupConfig
            );

        if (lookupConfig.isUseAsync()) {
            log.info("Using Async version of HttpLookupTable.");
            return AsyncTableFunctionProvider.of(
                new AsyncHttpTableLookupFunction(dataLookupFunction));
        } else {
            log.info("Using blocking version of HttpLookupTable.");
            return TableFunctionProvider.of(dataLookupFunction);
        }
    }

    @Override
    public DynamicTableSource copy() {
        return new HttpLookupTableSource(
            physicalRowDataType,
            lookupConfig,
            decodingFormat,
            dynamicTableFactoryContext
        );
    }

    @Override
    public String asSummaryString() {
        return "Http Lookup Table Source";
    }

    @Override
    public void applyLimit(long limit) {
    }

    @Override
    public boolean supportsNestedProjection() {
        return false;
    }

    private PollingClientFactory<RowData> createPollingClientFactory(
            LookupQueryCreator lookupQueryCreator,
            HttpLookupConfig lookupConfig) {

        boolean useRawAuthHeader =
            lookupConfig.getReadableConfig().get(HttpLookupConnectorOptions.USE_RAW_AUTH_HEADER);

        HeaderPreprocessor headerPreprocessor =
            HttpHeaderUtils.createDefaultHeaderPreprocessor(useRawAuthHeader);
        String lookupMethod = lookupConfig.getLookupMethod();

        HttpRequestFactory requestFactory = (lookupMethod.equalsIgnoreCase("GET")) ?
            new GetRequestFactory(
                lookupQueryCreator,
                headerPreprocessor,
                lookupConfig) :
            new BodyBasedRequestFactory(
                lookupMethod,
                lookupQueryCreator,
                headerPreprocessor,
                lookupConfig
            );

        return new JavaNetHttpPollingClientFactory(requestFactory);
    }

    private LookupRow extractLookupRow(int[][] keys) {

        LookupRow lookupRow = new LookupRow();

        List<String> fieldNames =
            TableSourceHelper.getFieldNames(physicalRowDataType.getLogicalType());

        List<LogicalType> fieldTypes =
            LogicalTypeChecks.getFieldTypes(physicalRowDataType.getLogicalType());

        List<Field> lookupDataTypes = new ArrayList<>();
        List<DataType> physicalRowDataTypes = physicalRowDataType.getChildren();

        int i = 0;
        for (int[] key : keys) {
            for (int keyIndex : key) {
                LogicalType type = fieldTypes.get(keyIndex);
                String name = fieldNames.get(keyIndex);
                lookupDataTypes.add(DataTypes.FIELD(name, physicalRowDataTypes.get(keyIndex)));
                lookupRow.addLookupEntry(extractKeyColumn(name, i++, type));
            }
        }

        lookupRow.setLookupPhysicalRowDataType(row(lookupDataTypes));
        return lookupRow;
    }

    private LookupSchemaEntry<RowData> extractKeyColumn(
        String name,
        int parentIndex,
        LogicalType type) {

        if (type instanceof RowType) {
            RowTypeLookupSchemaEntry rowLookupEntry = new RowTypeLookupSchemaEntry(
                name,
                RowData.createFieldGetter(type, parentIndex)
            );
            List<RowField> fields = ((RowType) type).getFields();
            int index = 0;
            for (RowField rowField : fields) {
                rowLookupEntry.addLookupEntry(processRow(rowField, index++));
            }
            return rowLookupEntry;
        } else {
            return new RowDataSingleValueLookupSchemaEntry(
                name,
                RowData.createFieldGetter(type, parentIndex)
            );
        }
    }

    private LookupSchemaEntry<RowData> processRow(RowField rowField, int parentIndex) {
        LogicalType type1 = rowField.getType();
        String name = rowField.getName();
        if (type1 instanceof RowType) {
            RowTypeLookupSchemaEntry rowLookupEntry = new RowTypeLookupSchemaEntry(name,
                RowData.createFieldGetter(type1, parentIndex));
            int index = 0;
            List<RowField> rowFields = ((RowType) type1).getFields();
            for (RowField rowField1 : rowFields) {
                rowLookupEntry.addLookupEntry(processRow(rowField1, index++));
            }
            return rowLookupEntry;
        } else {
            return new RowDataSingleValueLookupSchemaEntry(name,
                RowData.createFieldGetter(type1, parentIndex));
        }
    }
}
