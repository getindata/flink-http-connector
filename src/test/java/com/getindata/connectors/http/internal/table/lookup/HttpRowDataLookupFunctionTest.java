package com.getindata.connectors.http.internal.table.lookup;

import java.util.List;

import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryStringData;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.util.Collector;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.getindata.connectors.http.internal.PollingClient;
import com.getindata.connectors.http.internal.PollingClientFactory;
import com.getindata.connectors.http.internal.table.lookup.HttpTableLookupFunction.ColumnData;

@ExtendWith(MockitoExtension.class)
class HttpRowDataLookupFunctionTest {

    @Mock
    private PollingClientFactory<RowData> pollingClientFactory;

    @Mock
    private FunctionContext context;

    @Mock
    private Collector<RowData> collector;

    @Mock
    private PollingClient<RowData> client;

    @Captor
    private ArgumentCaptor<List<LookupArg>> lookupArgCaptor;

    private HttpTableLookupFunction lookupFunction;

    @BeforeEach
    void setUp() throws Exception {
        Mockito.when(context.getMetricGroup()).thenReturn(mock(MetricGroup.class));

        HttpLookupConfig options =
            HttpLookupConfig.builder().columnNames(List.of("id, uuid")).build();
        ColumnData columnData =
            ColumnData.builder().keyNames((List.of("id", "uuid").toArray(new String[2]))).build();

        when(pollingClientFactory.createPollClient(context, options)).thenReturn(client);

        lookupFunction = new HttpTableLookupFunction(pollingClientFactory, columnData, options);
        lookupFunction.open(context);
        lookupFunction.setCollector(collector);
    }

    @Test
    void shouldProcessAllKeys() {
        lookupFunction.eval(BinaryStringData.fromString("1"), BinaryStringData.fromString("2"));

        verify(client).pull(lookupArgCaptor.capture());
        List<LookupArg> arguments = lookupArgCaptor.getValue();

        assertThat(arguments.size()).isEqualTo(2);
        assertThat(arguments.get(0).getArgName()).isEqualTo("id");
        assertThat(arguments.get(0).getArgValue()).isEqualTo("1");
        assertThat(arguments.get(1).getArgName()).isEqualTo("uuid");
        assertThat(arguments.get(1).getArgValue()).isEqualTo("2");
    }

    @Test
    void shouldHandleUnsupportedType() {
        lookupFunction.eval(BinaryStringData.fromString("1"), 2);

        verify(client).pull(lookupArgCaptor.capture());
        List<LookupArg> arguments = lookupArgCaptor.getValue();

        assertThat(arguments.size()).isEqualTo(2);
        assertThat(arguments.get(0).getArgName()).isEqualTo("id");
        assertThat(arguments.get(0).getArgValue()).isEqualTo("1");
        assertThat(arguments.get(1).getArgName()).isEqualTo("uuid");
        assertThat(arguments.get(1).getArgValue()).isEqualTo("2");
    }
}
