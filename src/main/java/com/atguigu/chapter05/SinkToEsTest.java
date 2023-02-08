package com.atguigu.chapter05;

import org.apache.flink.connector.elasticsearch.sink.Elasticsearch7SinkBuilder;
import org.apache.flink.connector.elasticsearch.sink.ElasticsearchSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.util.HashMap;
import java.util.Map;

public class SinkToEsTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<Event> stream = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L),
                new Event("Alice", "./prod?id=100", 3000L),
                new Event("Alice", "./prod?id=200", 3500L),
                new Event("Bob", "./prod?id=2", 2500L),
                new Event("Alice", "./prod?id=300", 3600L),
                new Event("Bob", "./home", 3000L),
                new Event("Bob", "./prod?id=1", 2300L),
                new Event("Bob", "./prod?id=3", 3300L));
//        ArrayList<HttpHost> httpHosts = new ArrayList<>();
//        httpHosts.add(new HttpHost("hadoop102", 9200, "http"));
        // 创建一个ElasticsearchSinkFunction
//        ElasticsearchSinkFunction<Event> elasticsearchSinkFunction = new ElasticsearchSinkFunction<Event>() {
//            @Override
//            public void process(Event element, RuntimeContext ctx, RequestIndexer indexer) {
//                HashMap<String, String> data = new HashMap<>();
//                data.put(element.user, element.url);
//                IndexRequest request = Requests.indexRequest()
//                        .index("clicks")
//                        .type("type")    // Es 6 必须定义 type
//                        .source(data);
//                indexer.add(request);
//            }
//        };

        ElasticsearchSink<Event> sink = new Elasticsearch7SinkBuilder<Event>()
                .setBulkFlushMaxActions(1) // Instructs the sink to emit after every element, otherwise they would be buffered
                .setHosts(new HttpHost("127.0.0.1", 9200, "http"))
                .setEmitter(
                        (element, context, indexer) ->
                                indexer.add(createIndexRequest(element)))
                .build();
//        stream.addSink(new ElasticsearchSink.Builder<Event>(httpHosts, elasticsearchSinkFunction).build());
        stream.sinkTo(sink);
        env.execute();
    }
    private static IndexRequest createIndexRequest(Event element) {
        Map<String, Object> json = new HashMap<>();
        json.put("data", element);
        return Requests.indexRequest()
                .index("my-index")
                .type("my-type")
                .id(element.user)
                .source(json);
    }
}
