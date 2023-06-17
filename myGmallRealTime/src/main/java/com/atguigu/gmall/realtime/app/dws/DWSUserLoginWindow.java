package com.atguigu.gmall.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.beans.UserLoginBean;
import com.atguigu.gmall.realtime.utils.DateFormatUtil;
import com.atguigu.gmall.realtime.utils.MyClickhouseUtil;
import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
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
 * DWS：用户域用户登陆各窗口汇总表
 */
public class DWSUserLoginWindow {
    public static void main(String[] args) throws Exception {
        //todo 创建流式处理
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置全局并行度：不设置默认为全并行度；1为单线程执行
        env.setParallelism(4);
        //检查点相关设置（略）
        //todo 从Kafka中读取DWD流量域事务事实表处理中的页面日志
        SingleOutputStreamOperator<UserLoginBean> reduce = env
                .fromSource(MyKafkaUtil.getKafkaSource("topic_page", "dws_user_user_login_window"), WatermarkStrategy.noWatermarks(), "kafka_source")
                //todo 对数据进行类型转换jsonStr -> jsonObj
                .map(JSON::parseObject)
                //todo 过滤出登录行为
                .filter(
                        (FilterFunction<JSONObject>) value -> {
                            String uid = value.getJSONObject("common").getString("uid");
                            String lastPageId = value.getJSONObject("page").getString("last_page_id");
                            return StringUtils.isNotEmpty(uid) && ("login".equals(lastPageId) || StringUtils.isEmpty(lastPageId));
                        }
                )
                //todo 设置Watermark
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<JSONObject>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner((SerializableTimestampAssigner<JSONObject>) (element, recordTimestamp) -> element.getLong("ts"))
                )
                //todo 按照uid分组
                .keyBy(value -> value.getJSONObject("common").getString("uid"))
                //todo 封装为实体类对象
                .process(
                        new KeyedProcessFunction<String, JSONObject, UserLoginBean>() {
                            private ValueState<String> lastLoginDateState;

                            @Override
                            public void open(Configuration parameters) {
                                ValueStateDescriptor<String> valueStateDescriptor = new ValueStateDescriptor<>("lastLoginDate", String.class);
                                //由于需要计算回流用户，这里不设置状态过期时间
                                lastLoginDateState = getRuntimeContext().getState(valueStateDescriptor);
                            }

                            @Override
                            public void processElement(JSONObject value, KeyedProcessFunction<String, JSONObject, UserLoginBean>.Context ctx, Collector<UserLoginBean> out) throws Exception {
                                String lastLoginDate = lastLoginDateState.value();
                                Long ts = value.getLong("ts");
                                String curLoginDate = DateFormatUtil.toDate(ts);
                                long backCt = 0L;
                                long uuCt = 0L;
                                if (StringUtils.isNotEmpty(lastLoginDate)) {
                                    //若上次登录日期不为null
                                    if (!lastLoginDate.equals(curLoginDate)) {
                                        //若上次登陆日期不等于当天
                                        uuCt = 1L;
                                        lastLoginDateState.update(curLoginDate);
                                        if (((ts - DateFormatUtil.toTs(lastLoginDate)) / 100 / 60 / 60 / 24) >= 8) {
                                            //若上次登陆日期和当天差值大于7，即回流用户
                                            backCt = 1L;
                                        }
                                    }
                                } else {
                                    //若上次登录日期为null
                                    uuCt = 1L;
                                    lastLoginDateState.update(curLoginDate);
                                }
                                //将满足上述条件的两种数据封装为自定义类
                                if (uuCt != 0L) {
                                    out.collect(
                                            new UserLoginBean(
                                                    "", "",
                                                    backCt,
                                                    uuCt,
                                                    ts
                                            )
                                    );
                                }
                            }
                        }
                )
                //todo 开窗、聚合，没有按照特定维度统计，无需keyBy，此时并行度为1
                .windowAll(TumblingEventTimeWindows.of(Time.seconds(10L)))
                .reduce(
                        (ReduceFunction<UserLoginBean>) (value1, value2) -> {
                            value1.setUuCt(value1.getUuCt() + value2.getUuCt());
                            value1.setBackCt(value1.getBackCt() + value2.getBackCt());
                            return value1;
                        },
                        new AllWindowFunction<UserLoginBean, UserLoginBean, TimeWindow>() {
                            @Override
                            public void apply(TimeWindow window, Iterable<UserLoginBean> values, Collector<UserLoginBean> out) {
                                String stt = DateFormatUtil.toYmdHms(window.getStart());
                                String edt = DateFormatUtil.toYmdHms(window.getEnd());
                                for (UserLoginBean element : values) {
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
        reduce.addSink(MyClickhouseUtil.getSinkFunction("insert into dws_user_user_login_window values(?,?,?,?,?)"));

        //启动程序执行
        env.execute();
    }
}
