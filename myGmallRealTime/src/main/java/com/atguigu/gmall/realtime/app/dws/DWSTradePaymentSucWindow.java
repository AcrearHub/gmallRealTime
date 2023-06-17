package com.atguigu.gmall.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.beans.TradePaymentBean;
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
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * DWS：交易域支付各窗口汇总表
 */
public class DWSTradePaymentSucWindow {
    public static void main(String[] args) throws Exception {
        //todo 创建流式处理
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置全局并行度：不设置默认为全并行度；1为单线程执行
        env.setParallelism(4);
        //检查点相关设置（略）
        //todo 从Kafka中读取DWS交易域支付各窗口汇总表
        SingleOutputStreamOperator<TradePaymentBean> reduce = env
                .fromSource(MyKafkaUtil.getKafkaSource("topic_dwd_trade_pay_detail_suc", "dws_trade_payment_suc_window"), WatermarkStrategy.noWatermarks(), "kafka_source")
                //todo 对数据进行类型转换jsonStr -> jsonObj
                .map(JSON::parseObject)
                //todo 设置Watermark，注意时间
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<JSONObject>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner((SerializableTimestampAssigner<JSONObject>) (element, recordTimestamp) -> element.getLong("ts") * 1000)
                )
                //todo 按照user_id分组
                .keyBy(s -> s.getString("user_id"))
                //todo 封装为实体类对象
                .process(
                        new KeyedProcessFunction<String, JSONObject, TradePaymentBean>() {
                            private ValueState<String> lastPaySucDateState;

                            @Override
                            public void open(Configuration parameters) {
                                ValueStateDescriptor<String> valueStateDescriptor = new ValueStateDescriptor<>("ValueStateDescriptor", String.class);
                                lastPaySucDateState = getRuntimeContext().getState(valueStateDescriptor);
                            }

                            @Override
                            public void processElement(JSONObject value, KeyedProcessFunction<String, JSONObject, TradePaymentBean>.Context ctx, Collector<TradePaymentBean> out) throws Exception {
                                String lastPaySucDate = lastPaySucDateState.value();
                                Long ts = value.getLong("ts") * 1000;
                                String curDate = DateFormatUtil.toDate(ts);
                                long paySucUuCount = 0L;
                                long firstPayUserCount = 0L;
                                if (StringUtils.isEmpty(lastPaySucDate)) {
                                    //若之前状态为null
                                    paySucUuCount = 1L;
                                    firstPayUserCount = 1L;
                                    lastPaySucDateState.update(curDate);
                                } else {
                                    //若之前状态不为null
                                    if (!lastPaySucDate.equals(curDate)) {
                                        //若之前状态不为当天
                                        paySucUuCount = 1L;
                                        lastPaySucDateState.update(curDate);
                                    }
                                }
                                if (paySucUuCount != 0) {
                                    out.collect(
                                            new TradePaymentBean("", "", paySucUuCount, firstPayUserCount, ts)
                                    );
                                }
                            }
                        }
                )
                //todo 开窗、聚合，没有按照特定维度统计，无需keyBy，此时并行度为1
                .windowAll(TumblingEventTimeWindows.of(Time.seconds(10L)))
                .reduce(
                        new ReduceFunction<TradePaymentBean>() {
                            @Override
                            public TradePaymentBean reduce(TradePaymentBean value1, TradePaymentBean value2) {
                                value1.setPaymentSucUniqueUserCount(value1.getPaymentSucUniqueUserCount() + value2.getPaymentSucUniqueUserCount());
                                value1.setPaymentSucNewUserCount(value1.getPaymentSucNewUserCount() + value2.getPaymentSucNewUserCount());
                                return value1;
                            }
                        },
                        new AllWindowFunction<TradePaymentBean, TradePaymentBean, TimeWindow>() {
                            @Override
                            public void apply(TimeWindow window, Iterable<TradePaymentBean> values, Collector<TradePaymentBean> out) {
                                String stt = DateFormatUtil.toYmdHms(window.getStart());
                                String edt = DateFormatUtil.toYmdHms(window.getEnd());
                                for (TradePaymentBean element : values) {
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
        reduce.addSink(MyClickhouseUtil.getSinkFunction("insert into dws_trade_payment_suc_window values(?,?,?,?,?)"));

        //启动程序执行
        env.execute();
    }
}
