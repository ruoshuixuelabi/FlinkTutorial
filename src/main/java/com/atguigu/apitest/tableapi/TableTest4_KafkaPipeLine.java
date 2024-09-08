//package com.atguigu.apitest.tableapi;
//
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.table.api.DataTypes;
//import org.apache.flink.table.api.Table;
//import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
//import org.apache.flink.table.descriptors.Schema;
//
//import static org.apache.flink.table.api.Expressions.$;
//
///**
// * @ClassName: TableTest4_KafkaPipeLine
// * @Description:
// * @Author: wushengran on 2020/11/13 14:01
// * @Version: 1.0
// */
//public class TableTest4_KafkaPipeLine {
//    public static void main(String[] args) throws Exception {
//        // 1. 创建环境
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(1);
//        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
//        String k = "CREATE TABLE inputTable (\n" +
//                "  `id` STRING,\n" +
//                "  `timestamp` BIGINT,\n" +
//                "  `temp` DOUBLE\n" +
//                ") WITH (\n" +
//                "  'connector' = 'kafka',\n" +
//                "  'topic' = 'sensor',\n" +
//                "  'properties.bootstrap.servers' = 'localhost:2181',\n" +
//                "  'properties.group.id' = 'testGroup',\n" +
//                "  'scan.startup.mode' = 'earliest-offset',\n" +
//                "  'format' = 'csv'\n" +
//                ")";
//        tableEnv.executeSql(k);
////        // 2. 连接Kafka，读取数据
//        // 3. 查询转换
//        // 简单转换
//        Table sensorTable = tableEnv.from("inputTable");
//        Table resultTable = sensorTable.select($("id"), $("temp")).filter($("id").isEqual("sensor_6"));
//        // 聚合统计
//        Table aggTable = sensorTable.groupBy($("id")).select($("id"), $("id").count(),
//                $("temp").avg().as("average"));
//        // 4. 建立kafka连接，输出到不同的topic下
//        String outputTable = "CREATE TABLE outputTable (\n" +
//                "  `id` STRING,\n" +
//                "  `temp` DOUBLE\n" +
//                ") WITH (\n" +
//                "  'connector' = 'kafka',\n" +
//                "  'topic' = 'sinktest',\n" +
//                "  'properties.bootstrap.servers' = 'localhost:2181',\n" +
//                "  'properties.group.id' = 'testGroup',\n" +
//                "  'scan.startup.mode' = 'earliest-offset',\n" +
//                "  'format' = 'csv'\n" +
//                ")";
//        tableEnv.executeSql(outputTable);
//        resultTable.executeInsert("outputTable");
//        env.execute();
//    }
//}