package com.getindata.connectors.http.table;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.base.table.AsyncDynamicTableSinkFactory;
import org.apache.flink.connector.base.table.sink.options.AsyncSinkConfigurationValidator;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.factories.FactoryUtil;

import java.util.Properties;
import java.util.Set;

import static com.getindata.connectors.http.table.HttpDynamicSinkConnectorOptions.INSERT_METHOD;
import static com.getindata.connectors.http.table.HttpDynamicSinkConnectorOptions.URL;

/** Factory for creating {@link HttpDynamicSink}. */
public class HttpDynamicTableSinkFactory extends AsyncDynamicTableSinkFactory {
  public static final String IDENTIFIER = "http-sink";

  @Override
  public DynamicTableSink createDynamicTableSink(Context context) {
    AsyncDynamicSinkContext factoryContext = new AsyncDynamicSinkContext(this, context);

    ReadableConfig readableConfig = factoryContext.getTableOptions();
    Properties properties = new AsyncSinkConfigurationValidator(readableConfig).getValidatedConfigurations();

    HttpDynamicSink.HttpDynamicTableSinkBuilder builder = new HttpDynamicSink.HttpDynamicTableSinkBuilder()
        .setSinkConfig(getSinkConfigOptions(readableConfig))
        .setEncodingFormat(factoryContext.getEncodingFormat())
        .setConsumedDataType(factoryContext.getPhysicalDataType());
    addAsyncOptionsToBuilder(properties, builder);

    return builder.build();
  }

  @Override
  public String factoryIdentifier() {
    return IDENTIFIER;
  }

  @Override
  public Set<ConfigOption<?>> requiredOptions() {
    return Set.of(URL, FactoryUtil.FORMAT);
  }

  @Override
  public Set<ConfigOption<?>> optionalOptions() {
    return Set.of(INSERT_METHOD);
  }

  /**
   * @param config a configuration object
   * @return the configuration for {@link HttpDynamicSink}
   */
  private HttpDynamicSinkConfig getSinkConfigOptions(ReadableConfig config) {
    return HttpDynamicSinkConfig
        .builder()
        .url(config.get(URL))
        .format(config.get(FactoryUtil.FORMAT))
        .insertMethod(config.get(INSERT_METHOD))
        .build();
  }
}
