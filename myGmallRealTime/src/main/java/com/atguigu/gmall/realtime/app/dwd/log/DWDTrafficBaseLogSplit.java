package com.atguigu.gmall.realtime.app.dwd.log;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.utils.DateFormatUtil;
import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * DWD流量域事务事实表处理
 */
public class DWDTrafficBaseLogSplit {
    public static void main(String[] args) throws Exception {
        //todo 创建流式处理
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置全局并行度：不设置默认为全并行度；1为单线程执行
        env.setParallelism(4);
        //检查点相关设置（略）

        //todo 从Kafka中读数据，ETL
        KafkaSource<String> kafkaSource = MyKafkaUtil.getKafkaSource("topic_log", "dwd_traffic_base_log_split_group");
        DataStreamSource<String> kafkaStrDs = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafka_source");
        //对流中数据进行类型转换：json->jsonObj，并进行ETL，定义标签，形成主流、侧流
        OutputTag<String> dirtyTag = new OutputTag<String>("dirty_tag"){};  //匿名内部类：防止泛型擦除，下同
        SingleOutputStreamOperator<JSONObject> process = kafkaStrDs
                .process(new ProcessFunction<String, JSONObject>() {
                    @Override
                    public void processElement(String value, ProcessFunction<String, JSONObject>.Context ctx, Collector<JSONObject> out) {
                        try {
                            JSONObject jsonObject = JSON.parseObject(value);
                            out.collect(jsonObject);
                        } catch (Exception e) {
                            //转换中发生异常，即数据不是JSON格式数据，则通过dirty_tag侧流输出
                            ctx.output(dirtyTag, value);
                        }
                    }
                });
        //process.print("主流");
        //process.getSideOutput(dirtyTag).print("这是脏数据侧流");

        //todo 将脏数据写入Kafka
        process.getSideOutput(dirtyTag).sinkTo(MyKafkaUtil.getKafkaSink("topic_dirty"));

        //todo 修复数据：新老访客状态标记的修复——成因：前段埋点相关数据的缓存is_new被用户手动清理
        SingleOutputStreamOperator<JSONObject> map = process
                .keyBy(s -> s.getJSONObject("common").getString("mid"))
                .map(new RichMapFunction<JSONObject, JSONObject>() {
                    private ValueState<String> lastVisitDateState;

                    @Override
                    public void open(Configuration parameters) {
                        ValueStateDescriptor<String> stringValueStateDescriptor = new ValueStateDescriptor<>("lastVisitDateState", String.class);
                        //设置TTL存活时间
//                        stringValueStateDescriptor.enableTimeToLive(
//                                StateTtlConfig
//                                        .newBuilder(Time.days(1))
//                                        .build()
//                        );
                        lastVisitDateState = getRuntimeContext().getState(stringValueStateDescriptor);
                    }

                    @Override
                    public JSONObject map(JSONObject value) throws Exception {
                        //如果is_new = 1 -> 标记为新访客
                        String isNew = value.getJSONObject("common").getString("is_new");
                        String date = DateFormatUtil.toDate(value.getLong("ts"));
                        String dateBeforeOneDay = DateFormatUtil.toDate(value.getLong("ts") - 24 * 60 * 60 * 1000);
                        if ("1".equals(isNew)) {
                            //通过键控状态是否为null进行判断
                            if (StringUtils.isEmpty(lastVisitDateState.value())) {
                                //状态为空，则将状态设置为今天
                                lastVisitDateState.update(date);
                            } else {
                                //状态不为空，判断两次日期是否同一天，若不是，则将is_new更改为0
                                if (!lastVisitDateState.value().equals(date)) {
                                    isNew = "0";
                                    value.getJSONObject("common").put("is_new", isNew);
                                }
                            }
                        } else {
                            if (StringUtils.isEmpty(lastVisitDateState.value())) {
                                //状态为空，可能数仓刚上线，要把日期设置为今天之前
                                lastVisitDateState.update(dateBeforeOneDay);
                            }
                        }
                        return value;
                    }
                });
                //map.print("数据修复操作");

        //todo 分流：将数据按照：页面、曝光、启动、动作、错误日志进行分流
        OutputTag<String> errTag = new OutputTag<String>("err_tag") {};
        OutputTag<String> startTag = new OutputTag<String>("start_tag") {};
        OutputTag<String> actionTag = new OutputTag<String>("action_tag") {};
        OutputTag<String> displayTag = new OutputTag<String>("display_tag") {};
        SingleOutputStreamOperator<String> pageDS = map
                .process(
                        new ProcessFunction<JSONObject, String>() {
                            @Override
                            public void processElement(JSONObject value, ProcessFunction<JSONObject, String>.Context ctx, Collector<String> out) {
                                //判断是否为错误输出流，并移除err的JSON字段
                                if (value.getJSONObject("err") != null) {
                                    ctx.output(errTag, value.toJSONString());
                                    value.remove("err");
                                }
                                //判断是否为启动输出流
                                if (value.getJSONObject("start") != null) {
                                    ctx.output(startTag, value.toJSONString());
                                } else {    //不是启动日志，一定是页面日志
                                    JSONObject common = value.getJSONObject("common");
                                    JSONObject page = value.getJSONObject("page");
                                    Long ts = value.getLong("ts");
                                    //判断是否有曝光信息（这里不是JSONObject，而是JSONArray），并移除
                                    if (value.getJSONArray("displays") != null && value.getJSONArray("displays").size() > 0) {
                                        for (int i = 0; i < value.getJSONArray("displays").size(); i++) {
                                            JSONObject displays = value.getJSONArray("displays").getJSONObject(i);
                                            //封装曝光信息
                                            JSONObject jsonObject = new JSONObject();
                                            jsonObject.put("common", common);
                                            jsonObject.put("page", page);
                                            jsonObject.put("ts", ts);
                                            jsonObject.put("display", displays);
                                            ctx.output(displayTag, jsonObject.toJSONString());
                                        }
                                        value.remove("displays");
                                    }
                                    //判断是否有动作信息（这里不是JSONObject，而是JSONArray），并移除
                                    if (value.getJSONArray("actions") != null && value.getJSONArray("actions").size() > 0) {
                                        for (int i = 0; i < value.getJSONArray("actions").size(); i++) {
                                            JSONObject actions = value.getJSONArray("actions").getJSONObject(i);
                                            //封装动作信息（由于这里本来自带ts，不需要上传）
                                            JSONObject jsonObject = new JSONObject();
                                            jsonObject.put("common", common);
                                            jsonObject.put("page", page);
                                            jsonObject.put("action", actions);
                                            ctx.output(actionTag, jsonObject.toJSONString());
                                        }
                                        value.remove("actions");
                                    }
                                    //将页面日志输出到主流
                                    out.collect(value.toJSONString());
                                }
                            }
                        }
                );
        pageDS.print("这是page主流");
        pageDS.getSideOutput(errTag).print("这是err侧流");
        pageDS.getSideOutput(displayTag).print("这是display侧流");
        pageDS.getSideOutput(actionTag).print("这是action侧流");
        pageDS.getSideOutput(startTag).print("这是start侧流");

        //todo 将各个流数据写入Kafka
        pageDS.sinkTo(MyKafkaUtil.getKafkaSink("topic_page"));
        pageDS.getSideOutput(errTag).sinkTo(MyKafkaUtil.getKafkaSink("topic_err"));
        pageDS.getSideOutput(displayTag).sinkTo(MyKafkaUtil.getKafkaSink("topic_display"));
        pageDS.getSideOutput(actionTag).sinkTo(MyKafkaUtil.getKafkaSink("topic_action"));
        pageDS.getSideOutput(startTag).sinkTo(MyKafkaUtil.getKafkaSink("topic_start"));

        //启动程序执行
        env.execute();
    }
}
