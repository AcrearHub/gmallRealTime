package com.atguigu.gmall.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.beans.CartAddUuBean;
import com.atguigu.gmall.realtime.utils.DateFormatUtil;
import com.atguigu.gmall.realtime.utils.MyClickhouseUtil;
import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * DWS：交易域加购各窗口汇总表
 */
public class DWSTradeCartAddUuWindow {
    public static void main(String[] args) throws Exception {
        //todo 创建流式处理
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置全局并行度：不设置默认为全并行度；1为单线程执行
        env.setParallelism(4);
        //检查点相关设置（略）
        //todo 从Kafka中读取交易域加购事务事实表
        SingleOutputStreamOperator<CartAddUuBean> aggregate = env
                .fromSource(MyKafkaUtil.getKafkaSource("topic_dwd_trade_cart_add", "dws_trade_cart_add_group"), WatermarkStrategy.noWatermarks(), "kafka_source")
                //todo 对数据进行类型转换jsonStr -> jsonObj
                .map(JSON::parseObject)
                //todo 设置Watermark，注意时间ts要*1000
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<JSONObject>forBoundedOutOfOrderness(Duration.ZERO)
                                .withTimestampAssigner((SerializableTimestampAssigner<JSONObject>) (element, recordTimestamp) -> element.getLong("ts") * 1000)
                )
                //todo 按照user_id分组，这里是user_id，不是uid
                .keyBy(s -> s.getString("user_id"))
                //todo 过滤出所需数据，无需封装到自定义类
                .process(
                        new KeyedProcessFunction<String, JSONObject, JSONObject>() {
                            private ValueState<String> lastCartDateState;

                            @Override
                            public void open(Configuration parameters) {
                                ValueStateDescriptor<String> valueStateDescriptor = new ValueStateDescriptor<>("ValueStateDescriptor", String.class);
                                valueStateDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.days(1L)).build());
                                lastCartDateState = getRuntimeContext().getState(valueStateDescriptor);
                            }

                            @Override
                            public void processElement(JSONObject value, KeyedProcessFunction<String, JSONObject, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                                String lastCartDate = lastCartDateState.value();
                                Long ts = value.getLong("ts") * 1000;   //这里也要对ts进行转换
                                String curCartDate = DateFormatUtil.toDate(ts);
                                if (StringUtils.isEmpty(lastCartDate) || !lastCartDate.equals(curCartDate)) {
                                    //当天首次加购
                                    lastCartDateState.update(curCartDate);
                                    out.collect(value);
                                }
                            }
                        }
                )
                //todo 开窗、聚合计算
                .windowAll(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10L)))
                //前后类型不一致，使用aggregate
                .aggregate(
                        new AggregateFunction<JSONObject, Long, Long>() {
                            @Override
                            public Long createAccumulator() {
                                return 0L;
                            }

                            @Override
                            public Long add(JSONObject value, Long accumulator) {
                                return ++accumulator;   //这里不能用accumulator++！！
                            }

                            @Override
                            public Long getResult(Long accumulator) {
                                return accumulator;
                            }

                            @Override
                            public Long merge(Long a, Long b) { //会话窗口使用
                                return null;
                            }
                        },
                        //封装为实体类
                        new AllWindowFunction<Long, CartAddUuBean, TimeWindow>() {
                            @Override
                            public void apply(TimeWindow window, Iterable<Long> values, Collector<CartAddUuBean> out) throws Exception {
                                String stt = DateFormatUtil.toYmdHms(window.getStart());
                                String edt = DateFormatUtil.toYmdHms(window.getEnd());
                                for (Long value : values) {
                                    out.collect(new CartAddUuBean(stt, edt, value, System.currentTimeMillis()));
                                }
                            }
                        }
                );

        //todo 输出到CK
        aggregate.print();
        aggregate.addSink(MyClickhouseUtil.getSinkFunction("insert into dws_trade_cart_add_uu_window values(?,?,?,?)"));

        //启动程序执行
        env.execute();
    }
}
