package com.atguigu.gmall.realtime.app.dwd.db;

import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * DWD交易域支付成功事务事实表
 */
public class DWDTradePayDetailSuc {
    public static void main(String[] args) {
        //todo 创建流式处理
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置全局并行度：不设置默认为全并行度；1为单线程执行
        env.setParallelism(4);
        //检查点相关设置（略）
        //创建表执行环境
        StreamTableEnvironment streamTableEnvironment = StreamTableEnvironment.create(env);

        //todo 读取Kafka中topic_db的所有变化数据，形成表；由于使用interval join，增加Watermark
        streamTableEnvironment
                .executeSql(
                        "create table topic_db(" +
                                "`database` String,\n" +
                                "`table` String,\n" +
                                "`type` String,\n" +
                                "`data` map<String, String>,\n" +
                                "`old` map<String, String>,\n" +
                                "`proc_time` as PROCTIME(),\n" +
                                "`ts` string,\n" +
                                "row_time as TO_TIMESTAMP(FROM_UNIXTIME(cast(ts as bigint))),\n" +
                                "watermark for row_time as row_time" +
                                ")" + MyKafkaUtil.getKafkaDDL("topic_db","dwd_trade_pay_detail_suc")
                );

        //todo 从上表中过滤出支付成功相关数据，形成表
        Table paymentInfo = streamTableEnvironment
                .sqlQuery(
                        "select\n" +
                                "data['user_id'] user_id,\n" +
                                "data['order_id'] order_id,\n" +
                                "data['payment_type'] payment_type,\n" +
                                "data['callback_time'] callback_time,\n" +
                                "row_time,\n" +
                                "`proc_time`,\n" +
                                "ts\n" +
                                "from topic_db\n" +
                                "where `table` = 'payment_info'\n"
                );
        streamTableEnvironment.createTemporaryView("payment_info",paymentInfo);

        //todo 从DWD中拿到下单事务事实表相关数据，形成表
        streamTableEnvironment
                .executeSql(
                        "create table dwd_trade_order_detail(\n" +
                                "id string,\n" +
                                "order_id string,\n" +
                                "user_id string,\n" +
                                "sku_id string,\n" +
                                "sku_name string,\n" +
                                "province_id string,\n" +
                                "activity_id string,\n" +
                                "activity_rule_id string,\n" +
                                "coupon_id string,\n" +
                                "date_id string,\n" +
                                "create_time string,\n" +
                                "source_id string,\n" +
                                "source_type string,\n" +
                                "source_type_name string,\n" +
                                "sku_num string,\n" +
                                "split_original_amount string,\n" +
                                "split_activity_amount string,\n" +
                                "split_coupon_amount string,\n" +
                                "split_total_amount string,\n" +
                                "ts string,\n" +
                                "row_time as TO_TIMESTAMP(FROM_UNIXTIME(cast(ts as bigint))),\n" +
                                "watermark for row_time as row_time" +
                                ")" + MyKafkaUtil.getKafkaDDL("topic_dwd_trade_order_detail", "dwd_trade_pay_detail_suc")
                );

        //todo 拿到字典表
        streamTableEnvironment.executeSql(MyKafkaUtil.getBaseDicLookUpDDL());

        //todo 将上述3张表关联interval join——设置TTL设置Watermark、Timestamp（也可使用内连接，设置TTL），形成表
        Table joinedTable = streamTableEnvironment
                .sqlQuery(
                        "select\n" +
                                "pi.order_id order_id,\n" +
                                "pi.payment_type payment_type_code,\n" +
                                "dic.dic_name payment_type_name,\n" +
                                "pi.callback_time,\n" +
                                "pi.row_time,\n" +
                                "pi.ts\n" +
                                "from payment_info pi\n" +
                                "join `base_dic` for system_time as of pi.proc_time as dic\n" +
                                "on pi.payment_type = dic.dic_code"
                );
        streamTableEnvironment.createTemporaryView("joined_table",joinedTable);

        Table resultTable = streamTableEnvironment
                .sqlQuery(
                        "select\n" +
                                "od.id order_detail_id,\n" +
                                "od.order_id,\n" +
                                "od.user_id,\n" +
                                "od.sku_id,\n" +
                                "od.sku_name,\n" +
                                "od.province_id,\n" +
                                "od.activity_id,\n" +
                                "od.activity_rule_id,\n" +
                                "od.coupon_id,\n" +
                                "pi.payment_type_code,\n" +
                                "pi.payment_type_name,\n" +
                                "pi.callback_time,\n" +
                                "od.source_id,\n" +
                                "od.source_type source_type_code,\n" +
                                "od.source_type_name,\n" +
                                "od.sku_num,\n" +
                                "od.split_original_amount,\n" +
                                "od.split_activity_amount,\n" +
                                "od.split_coupon_amount,\n" +
                                "od.split_total_amount split_payment_amount,\n" +
                                "pi.ts \n" +
                                "from dwd_trade_order_detail od, joined_table pi\n" +
                                "where od.order_id = pi.order_id\n " +
                                "and od.row_time >= pi.row_time - INTERVAL '15' MINUTE \n" +
                                "and od.row_time <= pi.row_time + INTERVAL '5' SECOND"
                );
        streamTableEnvironment.createTemporaryView("result_table",resultTable);

        //todo 将关联结果写到Kafka中：upsert-kafka，形成表
        streamTableEnvironment
                .executeSql(
                        "create table dwd_trade_pay_detail_suc(\n" +
                                "order_detail_id string,\n" +
                                "order_id string,\n" +
                                "user_id string,\n" +
                                "sku_id string,\n" +
                                "sku_name string,\n" +
                                "province_id string,\n" +
                                "activity_id string,\n" +
                                "activity_rule_id string,\n" +
                                "coupon_id string,\n" +
                                "payment_type_code string,\n" +
                                "payment_type_name string,\n" +
                                "callback_time string,\n" +
                                "source_id string,\n" +
                                "source_type_code string,\n" +
                                "source_type_name string,\n" +
                                "sku_num string,\n" +
                                "split_original_amount string,\n" +
                                "split_activity_amount string,\n" +
                                "split_coupon_amount string,\n" +
                                "split_payment_amount string,\n" +
                                "ts string,\n" +
                                "primary key(order_detail_id) not enforced\n" +
                                ")" + MyKafkaUtil.getUpsertKafkaDDL("topic_dwd_trade_pay_detail_suc")
                );
        streamTableEnvironment.executeSql("insert into dwd_trade_pay_detail_suc select * from result_table");
    }
}
