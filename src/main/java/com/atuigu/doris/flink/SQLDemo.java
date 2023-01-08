package com.atuigu.doris.flink;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * TODO
 *
 * @author cjp
 * @version 1.0
 */
public class SQLDemo {
    public static void main(String[] args)  {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        tableEnv.executeSql("CREATE TABLE flink_doris (\n" +
                "    id INT,\n" +
                "    username STRING,\n" +
                "    password STRING\n" +
                "    ) \n" +
                "    WITH (\n" +
                "      'connector' = 'doris',\n" +
                "      'fenodes' = 'node01:8030',\n" +
                "      'table.identifier' = 'test_db.test_login',\n" +
                "      'username' = 'root',\n" +
                "      'password' = '123456'\n" +
                ")\n");


        // 读取数据
//        tableEnv.executeSql("select * from flink_doris").print();

        // 写入数据
        tableEnv.executeSql("insert into flink_doris(id,username,password) values(22,'root','123456')");

    }
}
