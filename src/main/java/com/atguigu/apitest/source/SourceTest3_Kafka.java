//package com.atguigu.apitest.source;
//
//import org.apache.flink.api.common.eventtime.WatermarkStrategy;
//import org.apache.flink.api.common.serialization.SimpleStringSchema;
//import org.apache.flink.connector.kafka.source.KafkaSource;
//import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
//import org.apache.flink.streaming.api.datastream.DataStreamSource;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//
//import java.util.Properties;
//
///**
// * @ClassName: SourceTest3_Kafka
// * @Description:
// * @Author: wushengran on 2020/11/7 11:54
// * @Version: 1.0
// */
//public class SourceTest3_Kafka {
//    public static void main(String[] args) throws Exception {
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(1);
//        Properties properties = new Properties();
//        properties.setProperty("bootstrap.servers", "localhost:9092");
//        properties.setProperty("group.id", "consumer-group");
//        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
//        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
//        properties.setProperty("auto.offset.reset", "latest");
//        // 从文件读取数据
////        DataStream<String> dataStream = env.addSource(new FlinkKafkaConsumer011<String>("sensor", new SimpleStringSchema(), properties));
//        KafkaSource<String> dataStream = KafkaSource.<String>builder()
//                .setBootstrapServers("localhost:9092")
//                .setTopics("sensor")
//                .setGroupId("consumer-group")
//                .setStartingOffsets(OffsetsInitializer.earliest())
//                .setValueOnlyDeserializer(new SimpleStringSchema())
//                .build();
//        DataStreamSource<String> kafkaSource = env.fromSource(dataStream, WatermarkStrategy.noWatermarks(), "Kafka Source");
//        // 打印输出
//        kafkaSource.print();
//        env.execute();
//    }
//}