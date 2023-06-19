package com.atguigu.gmall.realtime.app.dwd.db;

import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

/**
 * DWD交易域下单事务事实表
 */
public class DWDTradeOrderDetail {
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

        //todo 读到Kafka中topic_db的所有变化数据，形成表
        streamTableEnvironment.executeSql(MyKafkaUtil.fromTopicDbDDL("dwd_trade_order_group"));

        //todo 从上表中分别过滤出订单明细、订单、活动、优惠券相关数据，形成表
        //订单明细表
        Table orderDetail = streamTableEnvironment
                .sqlQuery(
                        "select \n" +
                                "data['id'] id,\n" +
                                "data['order_id'] order_id,\n" +
                                "data['sku_id'] sku_id,\n" +
                                "data['sku_name'] sku_name,\n" +
                                "data['create_time'] create_time,\n" +
                                "data['source_id'] source_id,\n" +
                                "data['source_type'] source_type,\n" +
                                "data['sku_num'] sku_num,\n" +
                                "cast(cast(data['sku_num'] as decimal(16,2)) * " +
                                "cast(data['order_price'] as decimal(16,2)) as String) split_original_amount,\n" +
                                "data['split_total_amount'] split_total_amount,\n" +
                                "data['split_activity_amount'] split_activity_amount,\n" +
                                "data['split_coupon_amount'] split_coupon_amount,\n" +
                                "ts \n" +
                                "from `topic_db` where `table` = 'order_detail' " +
                                "and `type` = 'insert'\n"
                );
        streamTableEnvironment.createTemporaryView("order_detail",orderDetail);

        //订单表（join）
        Table orderInfo = streamTableEnvironment
                .sqlQuery(
                        "select \n" +
                                "data['id'] id,\n" +
                                "data['user_id'] user_id,\n" +
                                "data['province_id'] province_id\n" +
                                "from `topic_db`\n" +
                                "where `table` = 'order_info'\n" +
                                "and `type` = 'insert'"
                );
        streamTableEnvironment.createTemporaryView("order_info",orderInfo);

        //活动表（left join）
        Table orderDetailActivity = streamTableEnvironment
                .sqlQuery(
                        "select \n" +
                                "data['order_detail_id'] order_detail_id,\n" +
                                "data['activity_id'] activity_id,\n" +
                                "data['activity_rule_id'] activity_rule_id\n" +
                                "from `topic_db`\n" +
                                "where `table` = 'order_detail_activity'\n" +
                                "and `type` = 'insert'\n"
                );
        streamTableEnvironment.createTemporaryView("order_detail_activity",orderDetailActivity);

        //优惠表（left join）
        Table orderDetailCoupon = streamTableEnvironment
                .sqlQuery(
                        "select\n" +
                                "data['order_detail_id'] order_detail_id,\n" +
                                "data['coupon_id'] coupon_id\n" +
                                "from `topic_db`\n" +
                                "where `table` = 'order_detail_coupon'\n" +
                                "and `type` = 'insert'\n"
                );
        streamTableEnvironment.createTemporaryView("order_detail_coupon",orderDetailCoupon);

        //todo 将上述4张表以明细表为主表关联join——设置TTL，形成表
        Table joinedTable = streamTableEnvironment
                .sqlQuery(
                        "select \n" +
                                "od.id,\n" +
                                "od.order_id,\n" +
                                "oi.user_id,\n" +
                                "od.sku_id,\n" +
                                "od.sku_name,\n" +
                                "oi.province_id,\n" +
                                "act.activity_id,\n" +
                                "act.activity_rule_id,\n" +
                                "cou.coupon_id,\n" +
                                "date_format(od.create_time, 'yyyy-MM-dd') date_id,\n" +
                                "od.create_time,\n" +
                                "od.sku_num,\n" +
                                "od.split_original_amount,\n" +
                                "od.split_activity_amount,\n" +
                                "od.split_coupon_amount,\n" +
                                "od.split_total_amount,\n" +
                                "od.ts \n" +
                                "from order_detail od \n" +
                                "join order_info oi\n" +
                                "on od.order_id = oi.id\n" +
                                "left join order_detail_activity act\n" +
                                "on od.id = act.order_detail_id\n" +
                                "left join order_detail_coupon cou\n" +
                                "on od.id = cou.order_detail_id"
                );
        streamTableEnvironment.createTemporaryView("joined_table",joinedTable);

        //todo 将关联结果写到Kafka中：upsert-kafka，形成表
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
                                "sku_num string,\n" +
                                "split_original_amount string,\n" +
                                "split_activity_amount string,\n" +
                                "split_coupon_amount string,\n" +
                                "split_total_amount string,\n" +
                                "ts string,\n" +
                                "primary key(id) not enforced\n" +
                                ")" + MyKafkaUtil.getUpsertKafkaDDL("topic_dwd_trade_order_detail")
                );
        streamTableEnvironment.executeSql("insert into dwd_trade_order_detail select * from joined_table");

    }
}
