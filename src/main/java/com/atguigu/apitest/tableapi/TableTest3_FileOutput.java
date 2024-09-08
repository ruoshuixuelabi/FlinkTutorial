//package com.atguigu.apitest.tableapi;
//
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.table.api.Table;
//import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
//
//import static org.apache.flink.table.api.Expressions.$;
//
///**
// * @ClassName: TableTest3_FileOutput
// * @Description:
// * @Author: wushengran on 2020/11/13 11:54
// * @Version: 1.0
// */
//public class TableTest3_FileOutput {
//    public static void main(String[] args) throws Exception {
//        // 1. 创建环境
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(1);
//        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
//        // 2. 表的创建：连接外部系统，读取数据
//        // 读取文件
//        String fileTable = "CREATE TABLE inputTable (\n" +
//                "  id STRING,\n" +
//                "  `timestamp` BIGINT,\n" +
//                "  temp DOUBLE\n" +
//                ") WITH (\n" +
//                "  'connector' = 'filesystem'," +
//                "  'path' = 'file:///E:\\mazhenkun\\FlinkTutorial\\src\\main\\resources\\hello2.txt', " +
//                "  'format' = 'csv'" +
//                ")";
//        tableEnv.executeSql(fileTable);
//        Table inputTable = tableEnv.from("inputTable");
//        // 3. 查询转换
//        // 3.1 Table API
//        Table resultTable = inputTable.select($("id"), $("temp")).filter($("id").isEqual("hello4"));
//        // 3.2 SQL
//        Table table = tableEnv.sqlQuery("select id, temp from inputTable where id = 'hello4'");
//        // 4. 输出到文件
//        // 连接外部文件注册输出表
//        String fileTable2 = "CREATE TABLE outputTable (\n" +
//                "  id STRING,\n" +
//                "  temp DOUBLE\n" +
//                ") WITH (\n" +
//                "  'connector' = 'filesystem'," +
//                "  'path' = 'file:///E:\\mazhenkun\\FlinkTutorial\\src\\main\\resources\\hello3.txt', " +
//                "  'format' = 'csv'" +
//                ")";
//        tableEnv.executeSql(fileTable2);
//        resultTable.executeInsert("outputTable");
//        String fileTable3 = "CREATE TABLE printoutputTable (\n" +
//                "  id STRING,\n" +
//                "  temp DOUBLE\n" +
//                ") WITH (\n" +
//                "  'connector' = 'print'" +
//                ")";
//        tableEnv.executeSql(fileTable3);
//        table.executeInsert("printoutputTable");
////        aggTable.insertInto("outputTable");
////        env.execute();
//    }
//}