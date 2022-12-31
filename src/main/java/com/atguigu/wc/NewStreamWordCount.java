package com.atguigu.wc;

import cn.hutool.core.io.resource.ClassPathResource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @ClassName: StreamWordCount
 * @Description:
 * @Author: wushengran on 2020/11/6 11:48
 * @Version: 1.0
 */
public class NewStreamWordCount {
    public static void main(String[] args) throws Exception {
        // 创建流处理执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置并行度
//        env.setParallelism(1);
//        // 从文件中读取数据
        ClassPathResource classPathResource = new ClassPathResource("hello.txt");
//        String inputPath = "D:\\Projects\\BigData\\FlinkTutorial\\src\\main\\resources\\hello.txt";
        String inputPath = classPathResource.getAbsolutePath();
//        FileSource<String> inputDataStream = FileSource.forRecordStreamFormat(new TextLineInputFormat(), new Path(inputPath)).build();
//                DataStream<String> inputDataStream = env.readTextFile(inputPath);
        // 用parameter tool工具从程序启动参数中提取配置项
//        ParameterTool parameterTool = ParameterTool.fromArgs(args);
//        String host = parameterTool.get("host");
//        int port = parameterTool.getInt("port");
        // 从socket文本流读取数据
        DataStream<String> inputDataStream = env.socketTextStream("hadoop2", 7777);
        // 基于数据流进行转换计算
        DataStream<Tuple2<String, Integer>> resultStream = inputDataStream.flatMap(new WordCount.MyFlatMapper())
                .keyBy(0)
                .sum(1)
                .setParallelism(2);
        resultStream.print().setParallelism(1);
//        // 执行任务
        System.out.println(inputDataStream);
        env.execute();
    }
}
