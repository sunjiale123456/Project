package SchemaUDF;

import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import io.debezium.data.Envelope;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.List;

public class MyDeserializationSchema implements DebeziumDeserializationSchema<String> {

    /*
    * {
    *  "db":"",
    *  "tableName":"",
    *  "before":{"id","name",""...},
    *  "after":{"id","name",""...},
    *  "op":""
    * }
    * */
    @Override
    public void deserialize(SourceRecord sourceRecord, Collector<String> collector) throws Exception {
        // 创建JSON对象用于封装结果数据
        JSONObject result = new JSONObject();

        // 获取库名 & 表名
        String topic = sourceRecord.topic();
        String[] split = topic.split("\\.");
        result.put("db",split[1]);
        result.put("tableName",split[2]);

        // 获取before
        Struct value = (Struct) sourceRecord.value();
        Struct before = value.getStruct("before");
        JSONObject beforeJson = new JSONObject();
        if(before != null){
            // 获取列信息
            Schema schema = before.schema();
            List<Field> fieldList = schema.fields();
            for(Field num: fieldList){
                beforeJson.put(num.name(),before.get(num));
            }
        }
        result.put("before",beforeJson);

        // 获取after信息
        Struct after = value.getStruct("after");
        JSONObject afterJson = new JSONObject();
        if(after !=null){
            // 获取列信息
            Schema schema = after.schema();
            List<Field> fieldList = schema.fields();
            for(Field num : fieldList){
                afterJson.put(num.name(),after.get(num));
            }
        }
        result.put("after",afterJson);

        // 获取操作类型
        Envelope.Operation operation = Envelope.operationFor(sourceRecord);
        result.put("op",operation);

        // 输出数据
        collector.collect(result.toJSONString());
    }

    @Override
    public TypeInformation<String> getProducedType() {
        return BasicTypeInfo.STRING_TYPE_INFO;
    }
}
