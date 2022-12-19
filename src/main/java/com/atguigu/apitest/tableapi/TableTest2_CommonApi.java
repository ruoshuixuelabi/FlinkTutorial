package com.atguigu.apitest.tableapi;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @ClassName: TableTest2_CommonApi
 * @Description:
 * @Author: wushengran on 2020/11/13 10:29
 * @Version: 1.0
 */
public class TableTest2_CommonApi {
    public static void main(String[] args) throws Exception {
        // 1. 创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        // 1.1 基于老版本planner的流处理
        EnvironmentSettings oldStreamSettings = EnvironmentSettings
                .newInstance()
//                .useOldPlanner()
                .inStreamingMode()
                .build();
        StreamTableEnvironment oldStreamTableEnv = StreamTableEnvironment.create(env, oldStreamSettings);
        // 1.2 基于老版本planner的批处理
//        ExecutionEnvironment batchEnv = ExecutionEnvironment.getExecutionEnvironment();
//        BatchTableEnvironment oldBatchTableEnv = BatchTableEnvironment.create(batchEnv);
        // 1.3 基于Blink的流处理
        EnvironmentSettings blinkStreamSettings = EnvironmentSettings.newInstance()
//                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        StreamTableEnvironment blinkStreamTableEnv = StreamTableEnvironment.create(env, blinkStreamSettings);
        // 1.4 基于Blink的批处理
        EnvironmentSettings blinkBatchSettings = EnvironmentSettings.newInstance()
//                .useBlinkPlanner()
                .inBatchMode()
                .build();
        TableEnvironment blinkBatchTableEnv = TableEnvironment.create(blinkBatchSettings);
        // 2. 表的创建：连接外部系统，读取数据
        // 2.1 读取文件
//        String filePath = "D:\\Projects\\BigData\\FlinkTutorial\\src\\main\\resources\\sensor.txt";
        String fileTable = "CREATE TABLE inputTable (\n" +
                "  id STRING,\n" +
                "  `timestamp` BIGINT,\n" +
                "  temp DOUBLE\n" +
                ") WITH (\n" +
                "  'connector' = 'filesystem'," +
                "  'path' = 'file:///E:\\mazhenkun\\FlinkTutorial\\src\\main\\resources\\hello2.txt', " +
                "  'format' = 'csv'" +
                ")";
//        tableEnv.connect(new FileSystem().path(filePath))
//                .withFormat(new Csv())
//                .withSchema(new Schema()
//                        .field("id", DataTypes.STRING())
//                        .field("timestamp", DataTypes.BIGINT())
//                        .field("temp", DataTypes.DOUBLE())
//                )
//                .createTemporaryTable("inputTable");
        tableEnv.executeSql(fileTable);
        Table inputTable = tableEnv.from("inputTable");
//        inputTable.printSchema();
//        tableEnv.toAppendStream(inputTable, Row.class).print();
        // 3. 查询转换
        // 3.1 Table API
        // 简单转换   tab.filter($("name").isEqual("Fred"));
        Table resultTable = inputTable.select($("id"), $("temp")).filter($("id").isEqual("hello4"));
        // 聚合统计   tab.groupBy($("key")).select($("key"), $("value").avg().plus(" The average").as("average"));
        Table aggTable = inputTable.groupBy($("id")).select($("id"), $("id").count(),
                $("temp").avg().as("average"));
        // 3.2 SQL
        tableEnv.sqlQuery("select id, temp from inputTable where id = 'hello4'");
        Table sqlAggTable = tableEnv.sqlQuery("select id, count(id) as cnt, avg(temp) as avgTemp from inputTable group by id");
        // 打印输出
        tableEnv.toAppendStream(resultTable, Row.class).print("result");
        tableEnv.toRetractStream(aggTable, Row.class).print("agg");
        tableEnv.toRetractStream(sqlAggTable, Row.class).print("sqlagg");
        env.execute();
    }
}