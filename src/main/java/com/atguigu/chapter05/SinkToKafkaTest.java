//package com.atguigu.chapter05;
//
//import org.apache.flink.api.common.serialization.SimpleStringSchema;
//import org.apache.flink.connector.base.DeliveryGuarantee;
//import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
//import org.apache.flink.connector.kafka.sink.KafkaSink;
//import org.apache.flink.streaming.api.datastream.DataStreamSource;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//
//import java.util.Properties;
//public class SinkToKafkaTest {
//    public static void main(String[] args) throws Exception {
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(1);
//        Properties properties = new Properties();
//        properties.put("bootstrap.servers", "hadoop102:9092");
//        DataStreamSource<String> stream = env.readTextFile("input/clicks.csv");
////        stream
////                .addSink(new FlinkKafkaProducer<String>(
////                        "clicks",
////                        new SimpleStringSchema(),
////                        properties
////                ));
//        //新版本下面
//        KafkaSink<String> sink = KafkaSink.<String>builder()
//                .setBootstrapServers("")
//                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
//                        .setTopic("topic-name")
//                        .setValueSerializationSchema(new SimpleStringSchema())
//                        .build()
//                )
//                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
//                .build();
//        stream.sinkTo(sink);
//        env.execute();
//    }
//}