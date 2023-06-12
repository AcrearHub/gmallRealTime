package com.atguigu.gmall.realtime.app.dwd.db;

import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * DWD交易域加购事实表
 */
public class DWDTradeCartAdd {
    public static void main(String[] args) {
        //todo 创建流式处理
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置全局并行度：不设置默认为全并行度；1为单线程执行
        env.setParallelism(4);
        //检查点相关设置（略）
        //创建表执行环境
        StreamTableEnvironment streamTableEnvironment = StreamTableEnvironment.create(env);

        //todo 读到Kafka中topic_db的所有变化数据，形成表
        streamTableEnvironment.executeSql(MyKafkaUtil.fromTopicDbDDL("dwd_trade_cart_add_group"));

        //todo 从上表中过滤出加购相关数据，形成表
        Table cartInfo = streamTableEnvironment
                .sqlQuery(
                        "select\n" +
                                "    `data`['id'] id,\n" +
                                "    `data`['user_id'] user_id,\n" +
                                "    `data`['sku_id'] sku_id,\n" +
                                "    if(`type`='insert',`data`['sku_num']," +   //若类型为插入，则直接取sku_num。否则用新数据-旧数据，后转String
                                "    cast((CAST(`data`['sku_num'] AS INT) - CAST(`old`['sku_num'] AS INT)) as string)) sku_num,\n" +
                                "    ts\n" +
                                "from\n" +
                                "    topic_db\n" +
                                "where \n" +    //过滤：表为cart_info，且type为插入，（或者更新且新数据比旧数据大且旧数据不为null）
                                "    `table`='cart_info' and (`type`='insert' \n" +
                                "    or (`type`='update' and (CAST(`data`['sku_num'] AS INT) > CAST(`old`['sku_num'] AS INT)) \n" +
                                "    and `old`['sku_num'] is not null))"
                );
        streamTableEnvironment.createTemporaryView("cart_info",cartInfo);

        //todo 将上表写到Kafka中：upsert-kafka，形成表
        streamTableEnvironment
                .executeSql(
                        "CREATE TABLE dwd_trade_cart_add (\n" +
                                "    id string,\n" +
                                "    user_id string,\n" +
                                "    sku_id string,\n" +
                                "    sku_num string,\n" +
                                "    ts string,\n" +
                                "    PRIMARY KEY (id) NOT ENFORCED\n" +
                                ") " + MyKafkaUtil.getUpsertKafkaDDL("topic_dwd_trade_cart_add")
                );
        streamTableEnvironment.executeSql("insert into dwd_trade_cart_add select * from cart_info");
    }
}
