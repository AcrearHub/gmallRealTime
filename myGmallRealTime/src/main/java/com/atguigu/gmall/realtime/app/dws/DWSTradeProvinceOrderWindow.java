package com.atguigu.gmall.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.beans.TradeProvinceOrderBean;
import com.atguigu.gmall.realtime.fuc.DimAsyncFunction;
import com.atguigu.gmall.realtime.utils.DateFormatUtil;
import com.atguigu.gmall.realtime.utils.MyClickhouseUtil;
import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.Collections;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;

/**
 * DWS：交易域省份粒度下单各窗口汇总表
 */
public class DWSTradeProvinceOrderWindow {
    public static void main(String[] args) throws Exception {
        //todo 创建流式处理
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置全局并行度：不设置默认为全并行度；1为单线程执行
        env.setParallelism(4);
        //检查点相关设置（略）
        //todo 从Kafka中读取订单明细表数据
        SingleOutputStreamOperator<TradeProvinceOrderBean> reduce = env
                .fromSource(MyKafkaUtil.getKafkaSource("topic_dwd_trade_order_detail", "dws_trade_province_order_window"), WatermarkStrategy.noWatermarks(), "kafka_source")
                //todo 过滤空消息，对数据进行类型转换jsonStr -> jsonObj
                .process(
                        new ProcessFunction<String, JSONObject>() {
                            @Override
                            public void processElement(String value, ProcessFunction<String, JSONObject>.Context ctx, Collector<JSONObject> out) {
                                if (StringUtils.isNotEmpty(value)) {
                                    out.collect(JSON.parseObject(value));
                                }
                            }
                        }
                )
                //todo 去重订单金额，状态 + 抵消
                .keyBy(s -> s.getString("id"))
                .process(
                        new KeyedProcessFunction<String, JSONObject, JSONObject>() {
                            private ValueState<JSONObject> lastValueState;

                            @Override
                            public void open(Configuration parameters) {
                                ValueStateDescriptor<JSONObject> valueStateDescriptor = new ValueStateDescriptor<>("lastValueState", JSONObject.class);
                                valueStateDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.seconds(10L)).build());
                                lastValueState = getRuntimeContext().getState(valueStateDescriptor);
                            }

                            @Override
                            public void processElement(JSONObject value, KeyedProcessFunction<String, JSONObject, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                                JSONObject lastValue = lastValueState.value();
                                if (lastValue != null) {
                                    //状态重复
                                    String splitTotalAmount = lastValue.getString("split_total_amount");
                                    lastValue.put("split_total_amount", "-" + splitTotalAmount);
                                    out.collect(lastValue);
                                }
                                out.collect(value);
                                lastValueState.update(value);
                            }
                        }
                )
                //todo 封装为实体类对象，去重订单数——Set集合方法
                .map(
                        new MapFunction<JSONObject, TradeProvinceOrderBean>() {
                            @Override
                            public TradeProvinceOrderBean map(JSONObject value) {
                                return TradeProvinceOrderBean.builder()
                                        .provinceId(value.getString("province_id"))
                                        .orderIdSet(new HashSet<>(Collections.singleton(value.getString("order_id"))))  //这里不能直接传入singleton方法的返回值，因为其返回的Set是不可变的
                                        .orderAmount(new BigDecimal(value.getString("split_total_amount")))
                                        .ts(value.getLong("ts") * 1000)
                                        .build();
                            }
                        }
                )
                //todo 设置Watermark
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<TradeProvinceOrderBean>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(
                                (SerializableTimestampAssigner<TradeProvinceOrderBean>) (element, recordTimestamp) -> element.getTs())
                )
                //todo 按照province_id分组、开窗、聚合
                .keyBy(TradeProvinceOrderBean::getProvinceId)
                .window(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10L)))
                .reduce(
                        new ReduceFunction<TradeProvinceOrderBean>() {
                            @Override
                            public TradeProvinceOrderBean reduce(TradeProvinceOrderBean value1, TradeProvinceOrderBean value2) {
                                value1.setOrderAmount(value1.getOrderAmount().add(value2.getOrderAmount()));
                                value1.getOrderIdSet().addAll(value2.getOrderIdSet());
                                return value1;
                            }
                        },
                        new WindowFunction<TradeProvinceOrderBean, TradeProvinceOrderBean, String, TimeWindow>() {
                            @Override
                            public void apply(String s, TimeWindow window, Iterable<TradeProvinceOrderBean> input, Collector<TradeProvinceOrderBean> out) {
                                String stt = DateFormatUtil.toYmdHms(window.getStart());
                                String edt = DateFormatUtil.toYmdHms(window.getEnd());
                                for (TradeProvinceOrderBean element : input) {
                                    element.setStt(stt);
                                    element.setEdt(edt);
                                    element.setOrderCount((long) element.getOrderIdSet().size());
                                    element.setTs(System.currentTimeMillis());
                                    out.collect(element);
                                }
                            }
                        }
                );
        //todo 从hbase中关联维度表
        SingleOutputStreamOperator<TradeProvinceOrderBean> name = AsyncDataStream.unorderedWait(
                reduce,
                new DimAsyncFunction<TradeProvinceOrderBean>("dim_base_province") {
                    @Override
                    public void join(TradeProvinceOrderBean input, JSONObject dimJSONObj) {
                        input.setProvinceName(dimJSONObj.getString("NAME"));
                    }

                    @Override
                    public String getKey(TradeProvinceOrderBean input) {
                        return input.getProvinceId();
                    }
                },
                60L, TimeUnit.SECONDS
        );
        //todo 输出到CK
        name.print();
        name.addSink(MyClickhouseUtil.getSinkFunction("insert into dws_trade_province_order_window values(?,?,?,?,?,?,?)"));

        //启动程序执行
        env.execute();
    }
}
