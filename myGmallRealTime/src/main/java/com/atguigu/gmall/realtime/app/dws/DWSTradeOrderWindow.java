package com.atguigu.gmall.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.beans.TradeOrderBean;
import com.atguigu.gmall.realtime.utils.DateFormatUtil;
import com.atguigu.gmall.realtime.utils.MyClickhouseUtil;
import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * DWS：交易域下单各窗口汇总表
 */
public class DWSTradeOrderWindow {
    public static void main(String[] args) throws Exception {
        //创建流式处理
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置全局并行度：不设置默认为全并行度；1为单线程执行
        env.setParallelism(1);
        //检查点相关设置（略）
        //todo 从Kafka中读取DWS交易域支付各窗口汇总表
        SingleOutputStreamOperator<TradeOrderBean> reduce = env
                .fromSource(MyKafkaUtil.getKafkaSource("topic_dwd_trade_order_detail", "dws_trade_order_window"), WatermarkStrategy.noWatermarks(), "kafka_source")
                //todo 过滤空消息，对数据进行类型转换jsonStr -> jsonObj
                .process(
                        new ProcessFunction<String, JSONObject>() {
                            @Override
                            public void processElement(String value, ProcessFunction<String, JSONObject>.Context ctx, Collector<JSONObject> out) {
                                if (value != null) {
                                    out.collect(JSON.parseObject(value));
                                }
                            }
                        }
                )
                //todo 设置Watermark，注意时间
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<JSONObject>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner((SerializableTimestampAssigner<JSONObject>) (element, recordTimestamp) -> element.getLong("ts") * 1000)
                )
                //todo 按照user_id分组
                .keyBy(s -> s.getString("user_id"))
                //todo 封装为实体类对象
                .process(
                        new KeyedProcessFunction<String, JSONObject, TradeOrderBean>() {
                            private ValueState<String> lastOrderDateState;

                            @Override
                            public void open(Configuration parameters) {
                                ValueStateDescriptor<String> valueStateDescriptor = new ValueStateDescriptor<>("ValueStateDescriptor", String.class);
                                lastOrderDateState = getRuntimeContext().getState(valueStateDescriptor);
                            }

                            @Override
                            public void processElement(JSONObject value, KeyedProcessFunction<String, JSONObject, TradeOrderBean>.Context ctx, Collector<TradeOrderBean> out) throws Exception {
                                String lastOrderDate = lastOrderDateState.value();
                                Long ts = value.getLong("ts");
                                String curDate = DateFormatUtil.toDate(ts);
                                long orderUuCount = 0L;
                                long firstOrderUserCount = 0L;
                                if (StringUtils.isEmpty(lastOrderDate)) {
                                    //若状态为空
                                    orderUuCount = 1L;
                                    firstOrderUserCount = 1L;
                                    lastOrderDateState.update(curDate);
                                } else {
                                    //若状态不为空
                                    if (!lastOrderDate.equals(curDate)) {
                                        //若状态中时间不等于当前时间
                                        orderUuCount = 1L;
                                        lastOrderDateState.update(curDate);
                                    }
                                }
                                if (orderUuCount != 0) {
                                    out.collect(
                                            new TradeOrderBean("", "", orderUuCount, firstOrderUserCount, ts)
                                    );
                                }
                            }
                        }
                )
                //todo 开窗、聚合，没有按照特定维度统计，无需keyBy，此时并行度为1
                .windowAll(TumblingEventTimeWindows.of(Time.seconds(10L)))
                .reduce(
                        new ReduceFunction<TradeOrderBean>() {
                            @Override
                            public TradeOrderBean reduce(TradeOrderBean value1, TradeOrderBean value2) {
                                value1.setOrderUniqueUserCount(value1.getOrderUniqueUserCount() + value2.getOrderUniqueUserCount());
                                value1.setOrderNewUserCount(value1.getOrderNewUserCount() + value2.getOrderNewUserCount());
                                return value1;
                            }
                        },
                        new AllWindowFunction<TradeOrderBean, TradeOrderBean, TimeWindow>() {
                            @Override
                            public void apply(TimeWindow window, Iterable<TradeOrderBean> values, Collector<TradeOrderBean> out) {
                                String stt = DateFormatUtil.toYmdHms(window.getStart());
                                String edt = DateFormatUtil.toYmdHms(window.getEnd());
                                for (TradeOrderBean element : values) {
                                    element.setStt(stt);
                                    element.setEdt(edt);
                                    element.setTs(System.currentTimeMillis());
                                    out.collect(element);
                                }
                            }
                        }
                );

        //todo 输出到CK
        reduce.print();
        reduce.addSink(MyClickhouseUtil.getSinkFunction("insert into dws_trade_order_window values(?,?,?,?,?)"));

        //启动程序执行
        env.execute();
    }
}
