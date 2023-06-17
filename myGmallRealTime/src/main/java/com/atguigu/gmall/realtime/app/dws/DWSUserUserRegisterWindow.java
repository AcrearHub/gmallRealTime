package com.atguigu.gmall.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.beans.UserRegisterBean;
import com.atguigu.gmall.realtime.utils.DateFormatUtil;
import com.atguigu.gmall.realtime.utils.MyClickhouseUtil;
import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * DWS：用户域用户注册各窗口汇总表
 */
public class DWSUserUserRegisterWindow {
    public static void main(String[] args) throws Exception {
        //todo 创建流式处理
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置全局并行度：不设置默认为全并行度；1为单线程执行
        env.setParallelism(4);
        //检查点相关设置（略）
        //todo 从Kafka中读取DWD流量域事务事实表处理中的页面日志
        SingleOutputStreamOperator<UserRegisterBean> aggregate = env
                .fromSource(MyKafkaUtil.getKafkaSource("topic_dwd_user_register", "dws_user_user_register_window"), WatermarkStrategy.noWatermarks(), "kafka_source")
                //todo 对数据进行类型转换jsonStr -> jsonObj
                .map(JSON::parseObject)
                //todo 设置Watermark
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<JSONObject>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner((SerializableTimestampAssigner<JSONObject>) (element, recordTimestamp) -> element.getLong("ts")*1000)    //注意时间
                )
                //todo 开窗、聚合，没有按照特定维度统计，无需keyBy，此时并行度为1
                .windowAll(TumblingEventTimeWindows.of(Time.seconds(10L)))
                .aggregate(
                        new AggregateFunction<JSONObject, Long, Long>() {
                            @Override
                            public Long createAccumulator() {
                                return 0L;
                            }

                            @Override
                            public Long add(JSONObject value, Long accumulator) {
                                return ++accumulator;
                            }

                            @Override
                            public Long getResult(Long accumulator) {
                                return accumulator;
                            }

                            @Override
                            public Long merge(Long a, Long b) {
                                return null;
                            }
                        },
                        new AllWindowFunction<Long, UserRegisterBean, TimeWindow>() {
                            @Override
                            public void apply(TimeWindow window, Iterable<Long> values, Collector<UserRegisterBean> out) {
                                String start = DateFormatUtil.toYmdHms(window.getStart());
                                String end = DateFormatUtil.toYmdHms(window.getEnd());
                                for (Long value : values) {
                                    out.collect(new UserRegisterBean(start, end, value, System.currentTimeMillis()));
                                }
                            }
                        }
                );

        //todo 输出到CK
        aggregate.print();
        aggregate.addSink(MyClickhouseUtil.getSinkFunction("insert into dws_user_user_register_window values(?,?,?,?)"));

        //启动程序执行
        env.execute();
    }
}
