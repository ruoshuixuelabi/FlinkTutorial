//package com.atguigu.chapter05;
//
//import org.apache.flink.api.common.serialization.SimpleStringEncoder;
//import org.apache.flink.configuration.MemorySize;
//import org.apache.flink.connector.file.sink.FileSink;
//import org.apache.flink.core.fs.Path;
//import org.apache.flink.streaming.api.datastream.DataStream;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
//
//import java.time.Duration;
//public class SinkToFileTest {
//    public static void main(String[] args) throws Exception{
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(4);
//        DataStream<Event> stream = env.fromElements(new Event("Mary", "./home", 1000L),
//                new Event("Bob", "./cart", 2000L),
//                new Event("Alice", "./prod?id=100", 3000L),
//                new Event("Alice", "./prod?id=200", 3500L),
//                new Event("Bob", "./prod?id=2", 2500L),
//                new Event("Alice", "./prod?id=300", 3600L),
//                new Event("Bob", "./home", 3000L),
//                new Event("Bob", "./prod?id=1", 2300L),
//                new Event("Bob", "./prod?id=3", 3300L));
////        StreamingFileSink<String> fileSink = StreamingFileSink
////                .<String>forRowFormat(new Path("./output"),
////                        new SimpleStringEncoder<>("UTF-8"))
////                .withRollingPolicy(
////                        DefaultRollingPolicy.builder()
////                                .withRolloverInterval(TimeUnit.MINUTES.toMillis(15))
////                                .withInactivityInterval(TimeUnit.MINUTES.toMillis(5))
////                                .withMaxPartSize(1024 * 1024 * 1024)
////                                .build())
////                .build();
//
//        final FileSink<String> sink = FileSink
//                .forRowFormat(new Path("./output"), new SimpleStringEncoder<String>("UTF-8"))
//                .withRollingPolicy(
//                        DefaultRollingPolicy.builder()
//                                .withRolloverInterval(Duration.ofMinutes(15))
//                                .withInactivityInterval(Duration.ofMinutes(5))
//                                .withMaxPartSize(MemorySize.ofMebiBytes(1024 * 1024 * 1024))
//                                .build())
//                .build();
//        // 将Event转换成String写入文件
////        stream.map(Event::toString).addSink(fileSink);
//        stream.map(Event::toString).sinkTo(sink);
//        env.execute();
//    }
//}