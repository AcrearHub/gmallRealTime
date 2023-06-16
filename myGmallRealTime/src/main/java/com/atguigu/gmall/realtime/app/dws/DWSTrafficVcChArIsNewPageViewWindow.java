package com.atguigu.gmall.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.beans.TrafficPageViewBean;
import com.atguigu.gmall.realtime.utils.DateFormatUtil;
import com.atguigu.gmall.realtime.utils.MyClickhouseUtil;
import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * DWS流量域版本-渠道-地区-访客类别粒度页面浏览各窗口汇总表
 */
public class DWSTrafficVcChArIsNewPageViewWindow {
    public static void main(String[] args) throws Exception {
        //todo 创建流式处理
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置全局并行度：不设置默认为全并行度；1为单线程执行
        env.setParallelism(4);
        //检查点相关设置（略）
        //todo 从Kafka中读取DWD流量域事务事实表处理中的页面日志
        KafkaSource<String> kafkaSource = MyKafkaUtil.getKafkaSource("topic_page", "dws_traffic_vc_ch_ar_isnew_group");
        SingleOutputStreamOperator<TrafficPageViewBean> reduce = env
                .fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafka_source")
                //todo 对流进行转换json
                .map(JSON::parseObject)
                //todo 按照mid分组
                .keyBy(s -> s.getJSONObject("common").getString("mid"))
                //todo 封装为实体类对象
                .process(
                        new ProcessFunction<JSONObject, TrafficPageViewBean>() {
                            private ValueState<String> lastVisitState;

                            @Override
                            public void open(Configuration parameters) throws Exception {
                                ValueStateDescriptor<String> valueStateDescriptor = new ValueStateDescriptor<>("valueStateDescriptor", String.class);
                                valueStateDescriptor.enableTimeToLive(
                                        StateTtlConfig
                                                .newBuilder(Time.days(1))
                                                .build()
                                );
                                lastVisitState = getRuntimeContext().getState(valueStateDescriptor);
                            }

                            @Override
                            public void processElement(JSONObject value, ProcessFunction<JSONObject, TrafficPageViewBean>.Context ctx, Collector<TrafficPageViewBean> out) throws Exception {
                                //从状态中获取上次访问日期
                                String lastVisit = lastVisitState.value();
                                //获取当前访问日期
                                String curVisit = DateFormatUtil.toDate(value.getLong("ts"));
                                long uvCount = 0L;
                                //判断是否为当天访问：没有访问日期，或者之前访问日期和今天不同
                                if (StringUtils.isEmpty(lastVisit) || !lastVisit.equals(curVisit)) {
                                    uvCount = 1L;
                                    lastVisitState.update(curVisit);    //将当前访问日期放到描述器重
                                }
                                String lastPageId = value.getJSONObject("page").getString("last_page_id");
                                long svCount = StringUtils.isEmpty(lastPageId) ? 1L : 0L;
                                TrafficPageViewBean trafficPageViewBean = new TrafficPageViewBean(
                                        "", "",
                                        value.getJSONObject("common").getString("vc"),
                                        value.getJSONObject("common").getString("ch"),
                                        value.getJSONObject("common").getString("ar"),
                                        value.getJSONObject("common").getString("is_new"),
                                        uvCount,    //独立访客数
                                        svCount,    //统计会话数
                                        1L,  //页面浏览数
                                        value.getJSONObject("page").getLong("during_time"),   //浏览总时长
                                        value.getLong("ts")
                                );
                                out.collect(trafficPageViewBean);
                            }
                        }
                )
                //todo 指定Watermark，提取时间字段
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<TrafficPageViewBean>forBoundedOutOfOrderness(Duration.ZERO)
                                .withTimestampAssigner((SerializableTimestampAssigner<TrafficPageViewBean>) (element, recordTimestamp) -> element.getTs())
                )
                //todo 按照维度分组、开窗、聚合
                .keyBy(
                        new KeySelector<TrafficPageViewBean, Tuple4<String,String,String,String>>() {
                            @Override
                            public Tuple4<String, String, String, String> getKey(TrafficPageViewBean value) throws Exception {
                                return Tuple4.of(
                                        value.getVc(), value.getCh(), value.getAr(), value.getIsNew());
                            }
                        }
                )
                .window(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10L)))
                .reduce(
                        (ReduceFunction<TrafficPageViewBean>) (value1, value2) -> {
                            value1.setUvCt(value1.getUvCt() + value2.getUvCt());
                            value1.setSvCt(value1.getSvCt() + value2.getSvCt());
                            value1.setPvCt(value1.getPvCt() + value2.getPvCt());
                            value1.setDurSum(value1.getDurSum() + value2.getDurSum());
                            return value1;
                        },
                        //将聚合后的数据补全：stt、edt、ts（这里的ts表示处理时间，将之前传入的ts覆盖掉）
                        new ProcessWindowFunction<TrafficPageViewBean, TrafficPageViewBean, Tuple4<String, String, String, String>, TimeWindow>() {
                            @Override
                            public void process(Tuple4<String, String, String, String> stringStringStringStringTuple4, ProcessWindowFunction<TrafficPageViewBean, TrafficPageViewBean, Tuple4<String, String, String, String>, TimeWindow>.Context context, Iterable<TrafficPageViewBean> elements, Collector<TrafficPageViewBean> out) throws Exception {
                                String stt = DateFormatUtil.toYmdHms(context.window().getStart());
                                String edt = DateFormatUtil.toYmdHms(context.window().getEnd());
                                for (TrafficPageViewBean element : elements) {
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
        reduce.addSink(MyClickhouseUtil.getSinkFunction("insert into dws_traffic_vc_ch_ar_is_new_page_view_window values(?,?,?,?,?,?,?,?,?,?,?)"));

        //启动程序执行
        env.execute();
    }
}






































