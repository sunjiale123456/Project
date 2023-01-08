package kafkaToDoris;

import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.cfg.DorisSink;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;

public class demo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"node01:9092");
        properties.put("auto.offset.reset","earliest");
        DataStreamSource<String> loginStream = env.addSource(new FlinkKafkaConsumer<String>(
                "kafka-login",
                new SimpleStringSchema(),
                properties
        ));
        loginStream.print();


//        Properties pro = new Properties();
//        pro.setProperty("format", "json");
//        pro.setProperty("strip_outer_array", "true");
//        loginStream.addSink(
//                DorisSink.sink(
//                        DorisReadOptions.builder().build(),
//                        DorisExecutionOptions.builder()
//                                .setBatchSize(3)
//                                .setBatchIntervalMs(0L)
//                                .setMaxRetries(3)
//                                .setStreamLoadProp(pro).build(),
//                        DorisOptions.builder()
//                                .setFenodes("node01:8030")
//                                .setTableIdentifier("test_db.test_login")
//                                .setUsername("root")
//                                .setPassword("123456").build()
//                ));

        env.execute();
    }
}
