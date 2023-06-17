package com.atguigu.gmall.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.beans.TrafficHomeDetailPageViewBean;
import com.atguigu.gmall.realtime.utils.DateFormatUtil;
import com.atguigu.gmall.realtime.utils.MyClickhouseUtil;
import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * DWS：流量域首页、详情页页面浏览各窗口汇总表
 */
public class DWSTrafficHomeDetailPageViewWindow {
    public static void main(String[] args) throws Exception {
        //todo 创建流式处理
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置全局并行度：不设置默认为全并行度；1为单线程执行
        env.setParallelism(4);
        //检查点相关设置（略）
        //todo 从Kafka中读取DWD流量域事务事实表处理中的页面日志
        SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> reduce = env
                .fromSource(MyKafkaUtil.getKafkaSource("topic_page", "dws_traffic_page_view_window"), WatermarkStrategy.noWatermarks(), "kafka_source")
                //todo 对数据进行类型转换jsonStr -> jsonObj
                .map(JSON::parseObject)
                //todo 过滤出首页、详情页日志
                .filter(
                        (FilterFunction<JSONObject>) value -> {
                            String page = value.getJSONObject("page").getString("page_id");
                            return page.equals("home") || page.equals("good_detail");
                        }
                )
                //todo 设置Watermark
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<JSONObject>forBoundedOutOfOrderness(Duration.ZERO)
                                .withTimestampAssigner((SerializableTimestampAssigner<JSONObject>) (element, recordTimestamp) -> element.getLong("ts"))
                )
                //todo 按照mid分组
                .keyBy(JSONObject -> JSONObject.getJSONObject("common").getString("mid"))
                //todo 封装为实体类对象
                .process(
                        new KeyedProcessFunction<String, JSONObject, TrafficHomeDetailPageViewBean>() {
                            private ValueState<String> homeLastVisitDateState;
                            private ValueState<String> datailLastVisitDateState;

                            @Override
                            public void open(Configuration parameters) {
                                ValueStateDescriptor<String> homevalueStateDescriptor = new ValueStateDescriptor<>("homevalueStateDescriptor", String.class);
                                ValueStateDescriptor<String> datailvalueStateDescriptor = new ValueStateDescriptor<>("datailvalueStateDescriptor", String.class);
                                homevalueStateDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.days(1L)).build());
                                datailvalueStateDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.days(1L)).build());
                                homeLastVisitDateState = getRuntimeContext().getState(homevalueStateDescriptor);
                                datailLastVisitDateState = getRuntimeContext().getState(datailvalueStateDescriptor);
                            }

                            @Override
                            public void processElement(JSONObject value, KeyedProcessFunction<String, JSONObject, TrafficHomeDetailPageViewBean>.Context ctx, Collector<TrafficHomeDetailPageViewBean> out) throws Exception {
                                String page = value.getJSONObject("page").getString("page_id");
                                Long ts = value.getLong("ts");
                                String curVisitDate = DateFormatUtil.toDate(ts);
                                long homeUvCount = 0L;
                                long detailUvCount = 0L;
                                //若page_id为home，存储日期不为当日或为null，将homeUvCount置为1，并更新日期为当日，否则置为0，不处理
                                if ("home".equals(page)) {
                                    String homeLastVisitDate = homeLastVisitDateState.value();
                                    if (StringUtils.isEmpty(homeLastVisitDate) || !homeLastVisitDate.equals(curVisitDate)) {
                                        homeUvCount = 1L;
                                        homeLastVisitDateState.update(curVisitDate);
                                    }
                                }
                                //若page_id为detail时同上
                                if ("good_detail".equals(page)) {
                                    String detailLastVisitDate = datailLastVisitDateState.value();
                                    if (StringUtils.isEmpty(detailLastVisitDate) || !detailLastVisitDate.equals(curVisitDate)) {
                                        detailUvCount = 1L;
                                        datailLastVisitDateState.update(curVisitDate);
                                    }
                                }
                                //将满足上述条件的两种数据封装为自定义类
                                if (homeUvCount != 0L || detailUvCount != 0L) {
                                    out.collect(new TrafficHomeDetailPageViewBean(
                                            "", "",
                                            homeUvCount,
                                            detailUvCount,
                                            ts
                                    ));
                                }
                            }
                        }
                )
                //todo 开窗、聚合，没有按照特定维度统计，无需keyBy，此时并行度为1
                .windowAll(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10L)))
                .reduce(
                        (ReduceFunction<TrafficHomeDetailPageViewBean>) (value1, value2) -> {
                            value1.setHomeUvCt(value1.getHomeUvCt() + value2.getHomeUvCt());
                            value1.setGoodDetailUvCt(value1.getGoodDetailUvCt() + value2.getGoodDetailUvCt());
                            return value1;
                        },
                        //这里也可用AllWindowFunction，只不过ProcessAllWindowFunction更底层，可操作性更强
                        new ProcessAllWindowFunction<TrafficHomeDetailPageViewBean, TrafficHomeDetailPageViewBean, TimeWindow>() {
                            @Override
                            public void process(ProcessAllWindowFunction<TrafficHomeDetailPageViewBean, TrafficHomeDetailPageViewBean, TimeWindow>.Context context, Iterable<TrafficHomeDetailPageViewBean> elements, Collector<TrafficHomeDetailPageViewBean> out) {
                                String stt = DateFormatUtil.toYmdHms(context.window().getStart());
                                String edt = DateFormatUtil.toYmdHms(context.window().getEnd());
                                for (TrafficHomeDetailPageViewBean element : elements) {
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
        reduce.addSink(MyClickhouseUtil.getSinkFunction("insert into dws_traffic_home_detail_page_view_window values(?,?,?,?,?)"));

        //启动程序执行
        env.execute();
    }
}
