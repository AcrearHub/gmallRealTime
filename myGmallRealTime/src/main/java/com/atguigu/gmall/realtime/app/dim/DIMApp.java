package com.atguigu.gmall.realtime.app.dim;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.beans.TableProcess;
import com.atguigu.gmall.realtime.fuc.DimSinkFunction;
import com.atguigu.gmall.realtime.fuc.TableProcessFunction;
import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/**
 * DIM维度层处理
 */
public class DIMApp {
    public static void main(String[] args) throws Exception {
        //todo 创建流式处理
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置全局并行度：不设置默认为全并行度；1为单线程执行
        env.setParallelism(4);
        /*
        //检查点相关设置
        //如果是精确一次，则必须开启检查点：👇
        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000L); //超时时间
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);  //Job取消后，保留检查点
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(2000L); //两个检查点间隔最短时间（防止检查点保存时间过长，导致连续备份检查点）
//      env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,3000L));   //重启策略：出错时最多重启3次，每次间隔3s
        env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.days(30),Time.seconds(3))); //重启策略：每30天有3次机会重启，每次重启间隔3s
        //设置状态后端
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop102:8020/checkpoint");
        //设置操作hadoop的用户
        System.setProperty("HADOOP_USER_NAME","atguigu");
         */

        //todo 从Kafka中读取数据
        String topic = "topic_db";
        String groupId = "dim_app_group";   //声明消费主题、消费者组
        KafkaSource<String> kafkaSource = MyKafkaUtil.getKafkaSource(topic, groupId);
        DataStreamSource<String> kafkaStrDs = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafka_source");    //开始消费,封装为流
        //对流中数据进行类型转换：json->jsonObj，并进行ETL，形成主流
        SingleOutputStreamOperator<JSONObject> jsonObject = kafkaStrDs
                .process(new ProcessFunction<String, JSONObject>() {
                    @Override
                    public void processElement(String value, ProcessFunction<String, JSONObject>.Context ctx, Collector<JSONObject> out) {
                        try {
                            JSONObject jsonObject = JSON.parseObject(value);
                            String type = jsonObject.getString("type");
                            //将开始和结束的type过滤掉（历史）
                            if (!"bootstrap-start".equals(type) && !"bootstrap-complete".equals(type)) {
                                out.collect(jsonObject);
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                });
        //jsonObject.print("写入数据");

        //todo 使用FlinkCDC读取配置表的数据
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

        //todo 将主流和广播流进行关联，再过滤出维度数据
        jsonObject
                .connect(broadcast)
                .process(new TableProcessFunction(mapStateDescriptor))
        //输出数据到hbase中
                .addSink(new DimSinkFunction());    //不能用JDBCSink的sink方法，它只能往一张表写

        //启动程序执行
        env.execute();
    }
}
