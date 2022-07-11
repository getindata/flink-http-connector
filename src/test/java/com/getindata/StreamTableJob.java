package com.getindata;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class StreamTableJob {

    public static void main(String[] args) {

        ParameterTool parameters = ParameterTool.fromSystemProperties();
        parameters = parameters.mergeWith(ParameterTool.fromArgs(args));

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // env.enableCheckpointing(5000);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(1000, 1000));
        env.setParallelism(1);
        env.disableOperatorChaining();
        env.getConfig().setGlobalJobParameters(parameters);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        tableEnv.executeSql(
            "CREATE TABLE Orders (id STRING, id2 STRING, proc_time AS PROCTIME())"
                + " WITH ("
                + "'connector' = 'datagen', 'rows-per-second' = '1', 'fields.id.kind' = 'sequence',"
                + " 'fields.id.start' = '1', 'fields.id.end' = '120',"
                + " 'fields.id2.kind' = 'sequence', 'fields.id2.start' = '2',"
                + " 'fields.id2.end' = '120')"
        );
        tableEnv.executeSql(
            "CREATE TABLE Customers (id STRING, id2 STRING, msg STRING, uuid STRING, isActive STRING, balance STRING) WITH ('connector' = 'rest-lookup', 'url' = 'http://localhost:8080/client', "
                + "'asyncPolling' = 'true', "
                + "'field.isActive.path' = '$.details.isActive', "
                + "'field.balance.path' = '$.details.nestedDetails.balance')");

        Table resultTable =
            tableEnv.sqlQuery(
                "SELECT o.id, o.id2, c.msg, c.uuid, c.isActive, c.balance FROM Orders AS o "
                    + "JOIN Customers FOR SYSTEM_TIME AS OF o.proc_time AS c "
                    + "ON o.id = c.id AND o.id2 = c.id2");

        /* DataStream<Row> rowDataStream = tableEnv.toDataStream(resultTable);
        rowDataStream.print();*/

        // Table result = tableEnv.sqlQuery("SELECT * FROM Orders");
        // Table result = tableEnv.sqlQuery("SELECT * FROM Customers");
        // Table result = tableEnv.sqlQuery("SELECT * FROM T WHERE T.id > 10");

        resultTable.execute().print();

        // env.execute();
    }
}
