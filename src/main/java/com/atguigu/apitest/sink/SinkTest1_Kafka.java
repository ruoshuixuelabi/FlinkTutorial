//package com.atguigu.apitest.sink;
//
//import com.atguigu.apitest.beans.SensorReading;
//import org.apache.flink.api.common.eventtime.WatermarkStrategy;
//import org.apache.flink.api.common.serialization.SimpleStringSchema;
//import org.apache.flink.api.connector.sink.Sink;
//import org.apache.flink.connector.base.DeliveryGuarantee;
//import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
//import org.apache.flink.connector.kafka.sink.KafkaSink;
//import org.apache.flink.connector.kafka.source.KafkaSource;
//import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
//import org.apache.flink.streaming.api.datastream.DataStream;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//
///**
// * @ClassName: SinkTest1_Kafka
// * @Description:
// * @Author: wushengran on 2020/11/9 10:24
// * @Version: 1.0
// */
//public class SinkTest1_Kafka {
//    public static void main(String[] args) throws Exception {
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(1);
////        // 从文件读取数据
////        DataStream<String> inputStream = env.readTextFile("D:\\Projects\\BigData\\FlinkTutorial\\src\\main\\resources\\sensor.txt");
////        Properties properties = new Properties();
////        properties.setProperty("bootstrap.servers", "localhost:9092");
////        properties.setProperty("group.id", "consumer-group");
////        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
////        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
////        properties.setProperty("auto.offset.reset", "latest");
//        // 从文件读取数据
////        DataStream<String> inputStream = env.addSource(new FlinkKafkaConsumer011<String>("sensor", new SimpleStringSchema(), properties));
//        KafkaSource<String> source = KafkaSource.<String>builder()
//                .setBootstrapServers("localhost:9092")
//                .setTopics("sensor")
//                .setGroupId("consumer-group")
//                .setStartingOffsets(OffsetsInitializer.earliest())
//                .setValueOnlyDeserializer(new SimpleStringSchema())
//                .build();
//        DataStream<String> inputStream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");
//        // 转换成SensorReading类型
//        DataStream<String> dataStream = inputStream.map(line -> {
//            String[] fields = line.split(",");
//            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2])).toString();
//        });
//        //旧版本使用下面的方式输出到Kafka
////        dataStream.addSink(new FlinkKafkaProducer011<String>("localhost:9092", "sinktest", new SimpleStringSchema()));
//        //新版本推荐KafkaSink
//        KafkaSink<String> sink = KafkaSink.<String>builder()
//                .setBootstrapServers("localhost:9092")
//                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
//                        .setTopic("sinktest")
//                        .setValueSerializationSchema(new SimpleStringSchema())
//                        .build()
//                )
//                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
//                .build();
//        dataStream.sinkTo((Sink<String, ?, ?, ?>) sink);
//        env.execute();
//    }
//}