//package com.atguigu.apitest.sink;
//
//import com.atguigu.apitest.beans.SensorReading;
//import org.apache.flink.api.common.functions.RuntimeContext;
//import org.apache.flink.api.connector.sink.Sink;
//import org.apache.flink.connector.elasticsearch.sink.Elasticsearch7SinkBuilder;
//import org.apache.flink.streaming.api.datastream.DataStream;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
//import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
//import org.apache.http.HttpHost;
//import org.elasticsearch.action.index.IndexRequest;
//import org.elasticsearch.client.Requests;
//
//import java.util.HashMap;
//
///**
// * @ClassName: SinkTest3_Es
// * @Description:
// * @Author: wushengran on 2020/11/9 11:21
// * @Version: 1.0
// */
//public class SinkTest3_Es {
//    public static void main(String[] args) throws Exception {
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(1);
//        // 从文件读取数据
//        DataStream<String> inputStream = env.readTextFile("D:\\Projects\\BigData\\FlinkTutorial\\src\\main\\resources\\sensor.txt");
//        // 转换成SensorReading类型
//        DataStream<SensorReading> dataStream = inputStream.map(line -> {
//            String[] fields = line.split(",");
//            return new SensorReading(fields[0], Long.valueOf(fields[1]), new Double(fields[2]));
//        });
//        dataStream.sinkTo(new Elasticsearch7SinkBuilder<SensorReading>()
//                // 下面的设置使 sink 在接收每个元素之后立即提交，否则这些元素将被缓存起来
//                .setBulkFlushMaxActions(1)
//                .setHosts(new HttpHost("127.0.0.1", 9200, "http"))
//                .setEmitter((element, context, indexer) -> indexer.add(createIndexRequest(element)))
//                .build());
////        // 定义es的连接配置  旧版本如下使用
////        ArrayList<HttpHost> httpHosts = new ArrayList<>();
////        httpHosts.add(new HttpHost("localhost", 9200));
////        dataStream.addSink(new ElasticsearchSink.Builder<SensorReading>(httpHosts, new MyEsSinkFunction()).build());
//        env.execute();
//    }
//
//    private static IndexRequest createIndexRequest(SensorReading element) {
//        // 定义写入的数据source
//        HashMap<String, String> dataSource = new HashMap<>();
//        dataSource.put("id", element.getId());
//        dataSource.put("temp", element.getTemperature().toString());
//        dataSource.put("ts", element.getTimestamp().toString());
//        return Requests.indexRequest()
//                .index("sensor")
//                .id(element.getId())
//                .source(dataSource);
//    }
//
//    // 实现自定义的ES写入操作
//    public static class MyEsSinkFunction implements ElasticsearchSinkFunction<SensorReading> {
//        @Override
//        public void process(SensorReading element, RuntimeContext ctx, RequestIndexer indexer) {
//            // 定义写入的数据source
//            HashMap<String, String> dataSource = new HashMap<>();
//            dataSource.put("id", element.getId());
//            dataSource.put("temp", element.getTemperature().toString());
//            dataSource.put("ts", element.getTimestamp().toString());
//            // 创建请求，作为向es发起的写入命令
//            IndexRequest indexRequest = Requests.indexRequest()
//                    .index("sensor")
//                    .type("readingdata")
//                    .source(dataSource);
//            // 用index发送请求
//            indexer.add(indexRequest);
//        }
//    }
//}