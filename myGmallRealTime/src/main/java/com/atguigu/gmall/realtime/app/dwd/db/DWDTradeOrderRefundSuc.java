package com.atguigu.gmall.realtime.app.dwd.db;

import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

/**
 * DWD交易域退款成功事务事实表
 */
public class DWDTradeOrderRefundSuc {
    public static void main(String[] args) {
        //todo 创建流式处理
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置全局并行度：不设置默认为全并行度；1为单线程执行
        env.setParallelism(4);
        //检查点相关设置（略）
        //创建表执行环境
        StreamTableEnvironment streamTableEnvironment = StreamTableEnvironment.create(env);
        //设置join表的TTL（单纯的数据传输延迟）
        streamTableEnvironment.getConfig().setIdleStateRetention(Duration.ofSeconds(10));

        //todo 读取Kafka中topic_db的所有变化数据，形成表
        streamTableEnvironment.executeSql(MyKafkaUtil.fromTopicDbDDL("dwd_trade_order_refund"));

        //todo 从上表中过滤出退款支付表中的退款成功、退单表中退款成功、订单表中的退款成功相关数据，形成表
        //退款支付表
        Table refundPayment = streamTableEnvironment
                .sqlQuery(
                        "select\n" +
                                "data['id'] id,\n" +
                                "data['order_id'] order_id,\n" +
                                "data['sku_id'] sku_id,\n" +
                                "data['payment_type'] payment_type,\n" +
                                "data['callback_time'] callback_time,\n" +
                                "data['total_amount'] total_amount,\n" +
                                "proc_time,\n" +
                                "ts\n" +
                                "from topic_db\n" +
                                "where `table` = 'refund_payment'\n"
                );
        streamTableEnvironment.createTemporaryView("refund_payment",refundPayment);

        //订单表
        Table orderInfo = streamTableEnvironment
                .sqlQuery(
                        "select\n" +
                                "data['id'] id,\n" +
                                "data['user_id'] user_id,\n" +
                                "data['province_id'] province_id,\n" +
                                "`old`\n" +
                                "from topic_db\n" +
                                "where `table` = 'order_info'\n" +
                                "and `type` = 'update'\n" +
                                "and data['order_status']='1006'\n" +
                                "and `old`['order_status'] is not null"
                );
        streamTableEnvironment.createTemporaryView("order_info",orderInfo);

        //退单表
        Table orderRefundInfo = streamTableEnvironment
                .sqlQuery(
                        "select\n" +
                                "data['order_id'] order_id,\n" +
                                "data['sku_id'] sku_id,\n" +
                                "data['refund_num'] refund_num,\n" +
                                "`old`\n" +
                                "from topic_db\n" +
                                "where `table` = 'order_refund_info'\n"
                );
        streamTableEnvironment.createTemporaryView("order_refund_info",orderRefundInfo);

        //todo 拿到字典表
        streamTableEnvironment.executeSql(MyKafkaUtil.getBaseDicLookUpDDL());

        //todo 将4张表关联join——设置TTL，形成表
        Table joinedTable = streamTableEnvironment
                .sqlQuery(
                        "select\n" +
                                "rp.id,\n" +
                                "oi.user_id,\n" +
                                "rp.order_id,\n" +
                                "rp.sku_id,\n" +
                                "oi.province_id,\n" +
                                "rp.payment_type,\n" +
                                "dic.dic_name payment_type_name,\n" +
                                "date_format(rp.callback_time,'yyyy-MM-dd') date_id,\n" +
                                "rp.callback_time,\n" +
                                "ri.refund_num,\n" +
                                "rp.total_amount,\n" +
                                "rp.ts,\n" +
                                "current_row_timestamp() row_op_ts\n" +
                                "from refund_payment rp \n" +
                                "join \n" +
                                "order_info oi\n" +
                                "on rp.order_id = oi.id\n" +
                                "join\n" +
                                "order_refund_info ri\n" +
                                "on rp.order_id = ri.order_id\n" +
                                "and rp.sku_id = ri.sku_id\n" +
                                " join \n" +
                                "base_dic for system_time as of rp.proc_time as dic\n" +
                                "on rp.payment_type = dic.dic_code\n"
                );
        streamTableEnvironment.createTemporaryView("joined_table",joinedTable);

        //todo 将关联结果写到Kafka中：upsert-kafka，形成表
        streamTableEnvironment
                .executeSql(
                        "create table dwd_trade_refund_pay_suc(\n" +
                                "id string,\n" +
                                "user_id string,\n" +
                                "order_id string,\n" +
                                "sku_id string,\n" +
                                "province_id string,\n" +
                                "payment_type_code string,\n" +
                                "payment_type_name string,\n" +
                                "date_id string,\n" +
                                "callback_time string,\n" +
                                "refund_num string,\n" +
                                "refund_amount string,\n" +
                                "ts string,\n" +
                                "row_op_ts timestamp_ltz(3),\n" +
                                "primary key(id) not enforced\n" +
                                ")" + MyKafkaUtil.getUpsertKafkaDDL("topic_dwd_trade_refund_pay_suc")
                );
        streamTableEnvironment.executeSql("insert into dwd_trade_refund_pay_suc select * from joined_table");
    }
}
