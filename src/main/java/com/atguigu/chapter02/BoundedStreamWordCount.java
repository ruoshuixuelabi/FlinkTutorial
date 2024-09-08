//package com.atguigu.chapter02;
//
//import org.apache.flink.api.common.eventtime.WatermarkStrategy;
//import org.apache.flink.api.common.typeinfo.Types;
//import org.apache.flink.api.java.tuple.Tuple2;
//import org.apache.flink.connector.file.src.FileSource;
//import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
//import org.apache.flink.core.fs.Path;
//import org.apache.flink.streaming.api.datastream.DataStream;
//import org.apache.flink.streaming.api.datastream.KeyedStream;
//import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.util.Collector;
//
//import java.util.Arrays;
//
//public class BoundedStreamWordCount {
//    public static void main(String[] args) throws Exception {
//        // 1. 创建流式执行环境
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        // 2. 读取文件
//        //下面方法已经废弃
////         DataStreamSource<String> lineDSS = env.readTextFile("input/words.txt");
//        //目前新版本推荐使用这种方法
//        FileSource<String> source = FileSource
//                .forRecordStreamFormat(new TextLineInputFormat(), new Path("input/words.txt"))
//                .build();
//        DataStream<String> stream = env.fromSource(
//                source,
//                WatermarkStrategy.noWatermarks(),
//                "MySourceName");
//        // 3. 转换数据格式
//        SingleOutputStreamOperator<Tuple2<String, Long>> wordAndOne = stream
//                .flatMap((String line, Collector<String> words) -> {
//                    Arrays.stream(line.split(" ")).forEach(words::collect);
//                })
//                .returns(Types.STRING)
//                .map(word -> Tuple2.of(word, 1L))
//                .returns(Types.TUPLE(Types.STRING, Types.LONG));
//        // 4. 分组
//        KeyedStream<Tuple2<String, Long>, String> wordAndOneKS = wordAndOne
//                .keyBy(t -> t.f0);
//        // 5. 求和
//        SingleOutputStreamOperator<Tuple2<String, Long>> result = wordAndOneKS
//                .sum(1);
//        // 6. 打印
//        result.print();
//        // 7. 执行
//        env.execute();
//    }
//}