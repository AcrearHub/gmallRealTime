package com.atguigu.gmall.realtime.app.dwd.db;

import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

/**
 * DWD交易域退单事务事实表
 */
public class DWDTradeOrderRefund {
    public static void main(String[] args) {
        //todo 创建流式处理
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置全局并行度：不设置默认为全并行度；1为单线程执行
        env.setParallelism(4);
        //检查点相关设置（略）
        //创建表执行环境
        StreamTableEnvironment streamTableEnvironment = StreamTableEnvironment.create(env);
        //设置join表的TTL（单纯数据延迟）
        streamTableEnvironment.getConfig().setIdleStateRetention(Duration.ofSeconds(10));

        //todo 读到Kafka中topic_db的所有变化数据，形成表
        streamTableEnvironment.executeSql(MyKafkaUtil.fromTopicDbDDL("dwd_trade_order_refund"));

        //todo 从上表中分别过滤出订单表中的退单、退单表相关数据，形成表
        //退单表
        Table orderRefondInfo = streamTableEnvironment
                .sqlQuery(
                        "select\n" +
                                "data['id'] id,\n" +
                                "data['user_id'] user_id,\n" +
                                "data['order_id'] order_id,\n" +
                                "data['sku_id'] sku_id,\n" +
                                "data['refund_type'] refund_type,\n" +
                                "data['refund_num'] refund_num,\n" +
                                "data['refund_amount'] refund_amount,\n" +
                                "data['refund_reason_type'] refund_reason_type,\n" +
                                "data['refund_reason_txt'] refund_reason_txt,\n" +
                                "data['create_time'] create_time,\n" +
                                "proc_time,\n" +
                                "ts\n" +
                                "from topic_db\n" +
                                "where `table` = 'order_refund_info'\n" +
                                "and `type` = 'insert'\n"
                );
        streamTableEnvironment.createTemporaryView("order_refund_info",orderRefondInfo);

        //订单表中的退单数据
        Table orderInfo = streamTableEnvironment
                .sqlQuery(
                        "select\n" +
                                "data['id'] id,\n" +
                                "data['province_id'] province_id,\n" +
                                "`old`\n" +
                                "from topic_db\n" +
                                "where `table` = 'order_info'\n" +
                                "and `type` = 'update'\n" +
                                "and data['order_status']='1005'\n" +
                                "and `old`['order_status'] is not null"
                );
        streamTableEnvironment.createTemporaryView("order_info",orderInfo);

        //todo 拿到字典表两次（退款类型名称 + 退款原因类型名称），形成表
        streamTableEnvironment.executeSql(MyKafkaUtil.getBaseDicLookUpDDL());

        //todo 将上述4张表以明细表为主表关联join——设置TTL，形成表
        Table joinedTable = streamTableEnvironment
                .sqlQuery(
                        "select \n" +
                                "ri.id,\n" +
                                "ri.user_id,\n" +
                                "ri.order_id,\n" +
                                "ri.sku_id,\n" +
                                "oi.province_id,\n" +
                                "date_format(ri.create_time,'yyyy-MM-dd') date_id,\n" +
                                "ri.create_time,\n" +
                                "ri.refund_type,\n" +
                                "type_dic.dic_name,\n" +
                                "ri.refund_reason_type,\n" +
                                "reason_dic.dic_name,\n" +
                                "ri.refund_reason_txt,\n" +
                                "ri.refund_num,\n" +
                                "ri.refund_amount,\n" +
                                "ri.ts,\n" +
                                "current_row_timestamp() row_op_ts\n" +
                                "from order_refund_info ri\n" +
                                "join \n" +
                                "order_info oi\n" +
                                "on ri.order_id = oi.id\n" +
                                "join \n" +
                                "base_dic for system_time as of ri.proc_time as type_dic\n" +
                                "on ri.refund_type = type_dic.dic_code\n" +
                                "join\n" +
                                "base_dic for system_time as of ri.proc_time as reason_dic\n" +
                                "on ri.refund_reason_type=reason_dic.dic_code"
                );
        streamTableEnvironment.createTemporaryView("joined_table",joinedTable);

        //todo 将关联结果写到Kafka中：upsert-kafka，形成表
        streamTableEnvironment
                .executeSql(
                        "create table dwd_trade_order_refund(\n" +
                                "id string,\n" +
                                "user_id string,\n" +
                                "order_id string,\n" +
                                "sku_id string,\n" +
                                "province_id string,\n" +
                                "date_id string,\n" +
                                "create_time string,\n" +
                                "refund_type_code string,\n" +
                                "refund_type_name string,\n" +
                                "refund_reason_type_code string,\n" +
                                "refund_reason_type_name string,\n" +
                                "refund_reason_txt string,\n" +
                                "refund_num string,\n" +
                                "refund_amount string,\n" +
                                "ts string,\n" +
                                "row_op_ts timestamp_ltz(3),\n" +
                                "primary key(id) not enforced\n" +
                                ")" + MyKafkaUtil.getUpsertKafkaDDL("topic_dwd_trade_order_refund")
                );
        streamTableEnvironment.executeSql("insert into dwd_trade_order_refund select * from joined_table");
    }
}
