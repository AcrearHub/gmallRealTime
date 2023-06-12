package com.atguigu.gmall.realtime.app.dwd.db;

import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * DWD互动域评论事务事实表
 */
public class DWDInteractionCommentInfo {
    public static void main(String[] args) {
        //todo 创建流式处理
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置全局并行度：不设置默认为全并行度；1为单线程执行
        env.setParallelism(4);
        //检查点相关设置（略）
        //创建表执行环境
        StreamTableEnvironment streamTableEnvironment = StreamTableEnvironment.create(env);

        //todo 读到Kafka中topic_db的所有变化数据，形成表
        streamTableEnvironment
                .executeSql(MyKafkaUtil.fromTopicDbDDL("dwd_interaction_comment_info_group"));

        //todo 从上表中过滤出评论相关数据，形成表
        Table commentInfo = streamTableEnvironment
                .sqlQuery(
                        "select " +
                                "data['id'] id," +
                                "data['user_id'] user_id," +
                                "data['sku_id'] sku_id," +
                                "data['appraise'] appraise," +
                                "data['comment_txt'] comment_txt," +
                                "ts," +
                                "proc_time" +
                                " from `topic_db` " +
                                " where `table` = 'comment_info'" +
                                " and `type` = 'insert'"
                );
        streamTableEnvironment.createTemporaryView("comment_info",commentInfo);

        //todo 从MySql读取字典表，形成表
        streamTableEnvironment.executeSql(MyKafkaUtil.getBaseDicLookUpDDL());

        //todo 对两张表进行lookup join，形成表
        Table joinedTable = streamTableEnvironment
                .sqlQuery(
                        "select " +
                                "ci.id," +
                                "user_id," +
                                "sku_id," +
                                "appraise," +
                                "dic_name appraise_name," +
                                "comment_txt," +
                                "ts" +
                                " from comment_info ci " +
                                " join base_dic for system_time as of ci.proc_time as dic " +
                                " on ci.appraise=dic.dic_code"
                );
        streamTableEnvironment.createTemporaryView("joined_table",joinedTable);

        //todo 将join后的表写到Kafka中：upsert-kafka，形成表
        streamTableEnvironment
                .executeSql(
                        "create table dwd_interaction_comment(" +
                                "id string," +
                                "user_id string," +
                                "sku_id string," +
                                "appraise string," +
                                "appraise_name string," +
                                "comment_txt string," +
                                "ts string," +
                                "primary key(id) not enforced" +
                                ")" + MyKafkaUtil.getUpsertKafkaDDL("topic_dwd_interaction_comment"));
        streamTableEnvironment.executeSql("insert into dwd_interaction_comment select * from joined_table");
    }
}
