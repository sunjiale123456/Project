package mysqlTokafka;

import SchemaUDF.MyDeserializationSchema;
import com.alibaba.fastjson.JSON;
import com.ververica.cdc.connectors.mysql.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.DebeziumSourceFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.json.JSONObject;
import pojo.Login;
import pojo.User;

import java.util.Properties;

public class FlinkCDCToKafka {
    public static void main(String[] args) throws Exception {
        // 1、获取 Flink 执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);

        // 2、通过 FlinkCDC构建SourceFunction
        DebeziumSourceFunction<String> sourceFunction = MySqlSource.<String>builder()
                .hostname("node01")
                .port(3306)
                .username("root")
                .password("123456")
                .databaseList("test,ke")    //监控的数据库
                .tableList("test.login,test.user,ke.ke_p_role")    //监控的数据库下的表
                .deserializer(new MyDeserializationSchema())//反序列化
                //earliest最早的 ,initial最初的 ,latest最近的，
                .startupOptions(StartupOptions.initial())
                .build();
        // 3、获取数据源
        DataStreamSource<String> dataStream = env.addSource(sourceFunction);

        //4、侧入输出流标签
        OutputTag<String> outputTagUser = new OutputTag<String>("user-Stream") {};
        OutputTag<String> outputTagKe_p_role = new OutputTag<String>("ke_p_role-Stream") {};

        // 5、将数据流根据不同的表，进行分流
        SingleOutputStreamOperator<String> processStream = dataStream.process(new ProcessFunction<String, String>() {
            @Override
            public void processElement(String s, Context ctx, Collector<String> out) throws Exception {
                JSONObject jsonObject = new JSONObject(s);
                String tableName = jsonObject.getString("tableName");
                JSONObject after = jsonObject.getJSONObject("after");
                String data = "{}".equals(after.toString()) ? "before" : "after";


                // 第一张表 login 主流
                if ("login".equals(tableName)) {
                    int id = jsonObject.getJSONObject(data).getInt("id");
                    String username = jsonObject.getJSONObject(data).getString("username");
                    String password = jsonObject.getJSONObject(data).getString("password");
                    Login login = new Login(id, username, password);
                    out.collect(JSON.toJSONString(login));
                }

                // 第二张表 ke_p_role 测流
                if ("ke_p_role".equals(tableName)) {
                    JSONObject object = jsonObject.getJSONObject(data);
                    ctx.output(outputTagKe_p_role, object.toString());
                }

                // 第三张表 user 测流
                if ("user".equals(tableName)) {
                    JSONObject object = jsonObject.getJSONObject(data);
                    User user = JSON.parseObject(object.toString(), User.class);
                    ctx.output(outputTagUser,user.toString());
                }
            }
        });

        // 6、将不同的流数据，写入到不同的Kafka的主题中
        processStream.print("login");
        processStream.getSideOutput(outputTagUser).print("user");
        processStream.getSideOutput(outputTagKe_p_role).print("Ke_p_role");

        // 写入kafka的user主题中

        // 主流 login 中的数据
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"node01:9092");
        processStream.addSink(new FlinkKafkaProducer<String>(
                "kafka-login",
                new SimpleStringSchema(),
                properties
        ));

        // 测流 user 中的数据
        FlinkKafkaProducer<String> producer = new FlinkKafkaProducer<>("kafka-user", new SimpleStringSchema(), properties);
        processStream.getSideOutput(outputTagUser).addSink(producer);
//
//        // 测流 role 中的数据
        processStream.getSideOutput(outputTagKe_p_role).addSink(new FlinkKafkaProducer<String>("kafka-role",new SimpleStringSchema(),properties));


        env.execute();

    }
}
