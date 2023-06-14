package com.atguigu.gmall.realtime.app.dwd.db;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.beans.TableProcess;
import com.atguigu.gmall.realtime.fuc.TableProcessFunciont_2;
import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * 事实表动态分流处理
 */
public class BaseDbApp {
    public static void main(String[] args) throws Exception {
        //todo 创建流式处理
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置全局并行度：不设置默认为全并行度；1为单线程执行
        env.setParallelism(4);
        //检查点相关设置（略）

        //todo 读到Kafka中topic_db的所有变化数据
        String topic = "topic_db";
        String groupId = "base_db_group";   //声明消费主题、消费者组
        KafkaSource<String> kafkaSource = MyKafkaUtil.getKafkaSource(topic, groupId);
        DataStreamSource<String> kafkaStrDs = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafka_source");

        //todo 对数据进行类型转换jsonStr -> jsonObj，并进行ETL
        SingleOutputStreamOperator<JSONObject> jsonObj = kafkaStrDs
                .process(
                        new ProcessFunction<String, JSONObject>() {
                            @Override
                            public void processElement(String value, ProcessFunction<String, JSONObject>.Context ctx, Collector<JSONObject> out) {
                                try {
                                    JSONObject jsonObject = JSON.parseObject(value);
                                    String type = jsonObject.getString("type");
                                    //过滤掉所有历史数据
                                    if (!type.startsWith("bootstrap-")) {
                                        out.collect(jsonObject);
                                    }
                                } catch (Exception e) {
                                    e.printStackTrace();
                                }
                            }
                        }
                );

        //todo 使用FlinkCDC读取配置表，并进行广播
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("hadoop102")
                .port(3306)
                .databaseList("gmall_realtime_config") // set captured database
                .tableList("gmall_realtime_config.table_process") // set captured table
                .username("root")
                .password("000000")
                .deserializer(new JsonDebeziumDeserializationSchema()) // converts SourceRecord to JSON String
                .build();
        DataStreamSource<String> mySqlStrDs = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySql Source");
        //对配置流进行广播：先建立配置描述器（只要名称相同，就是同一个描述器，通过key来获取需要更改的value）
        MapStateDescriptor<String, TableProcess> mapStateDescriptor = new MapStateDescriptor<>("mapStateDescriptor", String.class, TableProcess.class);
        BroadcastStream<String> broadcast = mySqlStrDs.broadcast(mapStateDescriptor);//对广播流的k：来源表名；v：表的每行

//        OutputTag<String> zhuceTag = new OutputTag<String>("zhuce_tag") {};
//        OutputTag<String> shoucangTag = new OutputTag<String>("shoucang_tag") {};
//        OutputTag<String> shiyongTag = new OutputTag<String>("shiyong_tag") {};

        //todo 主流、广播流进行关联，并进行处理
        SingleOutputStreamOperator<JSONObject> process = jsonObj
                .connect(broadcast)
                .process(new TableProcessFunciont_2(mapStateDescriptor));
//                .process(
//                        new ProcessFunction<JSONObject, String>() {
//                            @Override
//                            public void processElement(JSONObject value, ProcessFunction<JSONObject, String>.Context ctx, Collector<String> out) throws Exception {
//                                if ("dwd_user_register".equals(value.getString("sink_table"))) {
//                                    ctx.output(zhuceTag, value.toJSONString());
//                                }
//                                if ("dwd_tool_coupon_use".equals(value.getString("sink_table"))) {
//                                    ctx.output(shiyongTag, value.toJSONString());
//                                }
//                                if ("dwd_interaction_favor_add".equals(value.getString("sink_table"))) {
//                                    ctx.output(shoucangTag, value.toJSONString());
//                                }
//                                out.collect(value.toJSONString());
//                                value.remove("sink_table");
//                            }
//                        }
//                );

//        process.sinkTo(MyKafkaUtil.getKafkaSink("topic_dwd_tool_coupon_get"));
//        process.getSideOutput(zhuceTag).sinkTo(MyKafkaUtil.getKafkaSink("topic_dwd_user_register"));
//        process.getSideOutput(shoucangTag).sinkTo(MyKafkaUtil.getKafkaSink("topic_dwd_interaction_favor_add"));
//        process.getSideOutput(shiyongTag).sinkTo(MyKafkaUtil.getKafkaSink("topic_dwd_tool_coupon_use"));

        //todo 写到Kafka不同的主题中——这里也可以使用主侧流输出
        process.sinkTo(
                MyKafkaUtil.getKafkaSinkBySchema(
                        //序列化器：设定结果前往的分区，并将sink_table字段过滤掉
                        (KafkaRecordSerializationSchema<JSONObject>) (element, context, timestamp) -> {
                            String topic1 = element.getString("sink_table");
                            element.remove("sink_table");
                            return new ProducerRecord<>("topic_" + topic1, element.toJSONString().getBytes());
                        }
                )
        );

        //启动程序执行
        env.execute();
    }
}
