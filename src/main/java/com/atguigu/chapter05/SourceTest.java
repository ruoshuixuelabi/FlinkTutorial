//package com.atguigu.chapter05;
//
//import org.apache.flink.api.common.eventtime.WatermarkStrategy;
//import org.apache.flink.connector.file.src.FileSource;
//import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
//import org.apache.flink.core.fs.Path;
//import org.apache.flink.streaming.api.datastream.DataStream;
//import org.apache.flink.streaming.api.datastream.DataStreamSource;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//
//import java.util.ArrayList;
//
//public class SourceTest {
//    public static void main(String[] args) throws Exception {
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(1);
//        // 1. 从文件中读取数据
////        DataStreamSource<String> stream1 = env.readTextFile("input/clicks.csv");
//        FileSource<String> source = FileSource
//                .forRecordStreamFormat(new TextLineInputFormat(), new Path("input/clicks.csv"))
//                .build();
//        DataStream<String> stream1 = env.fromSource(
//                source,
//                WatermarkStrategy.noWatermarks(),
//                "MySourceName");
//        // 2. 从集合中读取数据
//        ArrayList<Integer> nums = new ArrayList<>();
//        nums.add(2);
//        nums.add(5);
//        DataStreamSource<Integer> numStream = env.fromCollection(nums);
//        ArrayList<Event> events = new ArrayList<>();
//        events.add(new Event("Mary", "./home", 1000L));
//        events.add(new Event("Bob", "./cart", 2000L));
//        DataStreamSource<Event> stream2 = env.fromCollection(events);
//        // 3. 从元素读取数据
//        DataStreamSource<Event> stream3 = env.fromElements(
//                new Event("Mary", "./home", 1000L),
//                new Event("Bob", "./cart", 2000L)
//        );
//        // 4. 从Socket文本流读取
//        DataStreamSource<String> stream4 = env.socketTextStream("localhost", 7777);
////        stream1.print("1");
////        numStream.print("nums");
////        stream2.print("2");
////        stream3.print("3");
//        stream4.print("4");
//        env.execute();
//    }
//}