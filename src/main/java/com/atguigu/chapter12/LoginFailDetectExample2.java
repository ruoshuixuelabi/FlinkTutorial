package com.atguigu.chapter12;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.nfa.aftermatch.AfterMatchSkipStrategy;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;

public class LoginFailDetectExample2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.setParallelism(1);
        // 1. 获取登录事件流，并提取时间戳、生成水位线
        KeyedStream<LoginEvent, String> stream = env
//                .fromElements( //新版本修改为fromData
                .fromData(
                        new LoginEvent("user_1", "192.168.0.1", "fail", 2000L),
                        new LoginEvent("user_2", "192.168.1.29", "fail", 4000L),
                        new LoginEvent("user_1", "192.168.0.2", "fail", 3000L),
                        new LoginEvent("user_2", "192.168.1.29", "fail", 4000L),
                        new LoginEvent("user_-1", "171.56.23.10", "fail", 5000L),
                        new LoginEvent("user_2", "192.168.1.29", "fail", 4000L),
                        new LoginEvent("user_1", "171.56.23.10", "fail", 5001L),
                        new LoginEvent("user_2", "192.168.1.29", "fail", 4000L),
                        new LoginEvent("user_2", "192.168.1.29", "fail", 7000L),
                        new LoginEvent("user_1", "171.56.23.10", "fail", 5002L),
                        new LoginEvent("user_2", "192.168.1.29", "fail", 4000L),
                        new LoginEvent("user_1", "171.56.23.10", "fail", 5003L),
                        new LoginEvent("user_1", "171.56.23.10", "fail", 5004L),
                        new LoginEvent("user_2", "192.168.1.29", "fail", 7000L),
                        new LoginEvent("user_2", "192.168.1.29", "fail", 8000L),
                        new LoginEvent("user_2", "192.168.1.29", "success", 6000L)
                )
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<LoginEvent>forBoundedOutOfOrderness(Duration.ofSeconds(30))
                                .withTimestampAssigner(
                                        (SerializableTimestampAssigner<LoginEvent>) (loginEvent, l) -> loginEvent.timestamp
                                )
                )
                .keyBy(r -> r.userId);
        // 2. 定义Pattern，连续的三个登录失败事件
        Pattern<LoginEvent, LoginEvent> pattern = Pattern.<LoginEvent>begin("first")    // 以第一个登录失败事件开始
                .where(new SimpleCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent loginEvent) throws Exception {
                        return loginEvent.eventType.equals("fail");
                    }
                })
                .timesOrMore(3);
//                .next("second")    // 接着是第二个登录失败事件
//                .where(new SimpleCondition<LoginEvent>() {
//                    @Override
//                    public boolean filter(LoginEvent loginEvent) throws Exception {
//                        return loginEvent.eventType.equals("fail");
//                    }
//                })
//                .next("third")     // 接着是第三个登录失败事件
//                .where(new SimpleCondition<LoginEvent>() {
//                    @Override
//                    public boolean filter(LoginEvent loginEvent) throws Exception {
//                        return loginEvent.eventType.equals("fail");
//                    }
//                });
//        // 3. 将Pattern应用到流上，检测匹配的复杂事件，得到一个PatternStream
        PatternStream<LoginEvent> patternStream = CEP.pattern(stream, pattern);
        // 4. 将匹配到的复杂事件选择出来，然后包装成字符串报警信息输出
        patternStream
                .select((PatternSelectFunction<LoginEvent, String>) map -> {
                    LoginEvent first = map.get("first").get(0);
                    LoginEvent second = map.get("first").get(1);
                    LoginEvent third = map.get("first").get(2);
//                    LoginEvent second = map.get("second").get(0);
//                    LoginEvent third = map.get("third").get(0);
                    return first.userId + " 连续三次登录失败！登录时间：" + map.get("first").size() + "，" + first.timestamp + ", " + second.timestamp + ", " + third.timestamp;
                })
                .print("warning");
        env.execute();
    }
}