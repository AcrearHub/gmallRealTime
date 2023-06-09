package com.atguigu.gmall.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.beans.TradeSkuOrderBean;
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
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * DWS：交易域SKU粒度下单各窗口汇总表
 */
public class DWSTradeSkuOrderWindow {
    public static void main(String[] args) throws Exception {
        //todo 创建流式处理
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置全局并行度：不设置默认为全并行度；1为单线程执行
        env.setParallelism(4);
        //检查点相关设置（略）
        //todo 从Kafka中读取订单明细表数据
        SingleOutputStreamOperator<TradeSkuOrderBean> reduce = env
                .fromSource(MyKafkaUtil.getKafkaSource("topic_dwd_trade_order_detail", "dws_trade_sku_order_group"), WatermarkStrategy.noWatermarks(), "kafka_source")
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
                //todo 去重消息，2中思路
                //按照订单id进行分组，再在组内去重
                .keyBy(s -> s.getString("id"))
                //1、状态 + 定时器方法：时效性不好
                /*
                .process(
                        new ProcessFunction<JSONObject, JSONObject>() {
                            private ValueState<JSONObject> lastValueState;

                            @Override
                            public void open(Configuration parameters) {
                                ValueStateDescriptor<JSONObject> valueStateDescriptor = new ValueStateDescriptor<>("lastValueState", JSONObject.class);
                                //TTL可以在定时器触发时来操作
                                lastValueState = getRuntimeContext().getState(valueStateDescriptor);
                            }

                            @Override
                            public void processElement(JSONObject value, ProcessFunction<JSONObject, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                                JSONObject lastValue = lastValueState.value();
                                if (lastValue != null){
                                    //状态中有值，出现重复
                                    //下面的两个时间应该使用DWD表的聚合时的时间，而不是ts！！！
                                    if (value.getLong("ts") > lastValue.getLong("ts")){
                                        lastValueState.update(value);
                                    }
                                } else {
                                    //状态中有值，没有重复，并注册一个5s的定时器
                                    lastValueState.update(value);
                                    long currentProcessingTime = ctx.timerService().currentProcessingTime();
                                    ctx.timerService().registerEventTimeTimer(currentProcessingTime + 5000L);
                                }
                            }

                            @Override
                            public void onTimer(long timestamp, ProcessFunction<JSONObject, JSONObject>.OnTimerContext ctx, Collector<JSONObject> out) throws Exception {
                                //定时器触发时执行，将状态中的数据传递到下游，并删除状态中的数据
                                out.collect(lastValueState.value());
                                lastValueState.clear();
                            }
                        }
                )*/
                //2、状态 + 抵消：传输的数据量变大
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
                                    //数据重复，将状态中影响到度量值的部分取反，往下游传递，形成抵消
                                    String splitOriginalAmount = lastValue.getString("split_original_amount");
                                    String splitActivityAmount = lastValue.getString("split_activity_amount");
                                    String splitCouponAmount = lastValue.getString("split_coupon_amount");
                                    String splitTotalAmount = lastValue.getString("split_total_amount");
                                    lastValue.put("split_original_amount", "-" + splitOriginalAmount);
                                    lastValue.put("split_activity_amount", "-" + splitActivityAmount);
                                    lastValue.put("split_coupon_amount", "-" + splitCouponAmount);
                                    lastValue.put("split_total_amount", "-" + splitTotalAmount);
                                    out.collect(lastValue);
                                }
                                out.collect(value);
                                lastValueState.update(value);
                            }
                        }
                )
                //todo 封装为实体类对象
                .map(
                        new MapFunction<JSONObject, TradeSkuOrderBean>() {
                            @Override
                            public TradeSkuOrderBean map(JSONObject value) {
                                return TradeSkuOrderBean
                                        .builder()
                                        .skuId(value.getString("sku_id"))
                                        .originalAmount(new BigDecimal(value.getString("split_original_amount")))
                                        .activityAmount(new BigDecimal(value.getString("split_activity_amount")))
                                        .couponAmount(new BigDecimal(value.getString("split_coupon_amount")))
                                        .orderAmount(new BigDecimal(value.getString("split_total_amount")))
                                        .ts(value.getLong("ts") * 1000)
                                        .build();
                            }
                        }
                )
                //todo 设置Watermark
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<TradeSkuOrderBean>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner((SerializableTimestampAssigner<TradeSkuOrderBean>) (element, recordTimestamp) -> element.getTs())
                )
                //todo 按照sku分组、开窗、聚合
                .keyBy(TradeSkuOrderBean::getSkuId)
                .window(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10L)))
                .reduce(
                        new ReduceFunction<TradeSkuOrderBean>() {
                            @Override
                            public TradeSkuOrderBean reduce(TradeSkuOrderBean value1, TradeSkuOrderBean value2) {
                                value1.setOriginalAmount(value1.getOriginalAmount().add(value2.getOriginalAmount()));
                                value1.setActivityAmount(value1.getActivityAmount().add(value2.getActivityAmount()));
                                value1.setCouponAmount(value1.getCouponAmount().add(value2.getCouponAmount()));
                                value1.setOrderAmount(value1.getOrderAmount().add(value2.getOrderAmount()));
                                return value1;
                            }
                        },
                        new ProcessWindowFunction<TradeSkuOrderBean, TradeSkuOrderBean, String, TimeWindow>() {
                            @Override
                            public void process(String s, ProcessWindowFunction<TradeSkuOrderBean, TradeSkuOrderBean, String, TimeWindow>.Context context, Iterable<TradeSkuOrderBean> elements, Collector<TradeSkuOrderBean> out) {
                                String stt = DateFormatUtil.toYmdHms(context.window().getStart());
                                String edt = DateFormatUtil.toYmdHms(context.window().getEnd());
                                for (TradeSkuOrderBean element : elements) {
                                    element.setStt(stt);
                                    element.setEdt(edt);
                                    element.setTs(System.currentTimeMillis());
                                    out.collect(element);
                                }
                            }
                        }
                );
        //todo 从hbase中关联商品sku、spu、品牌、类别3、类别2、类别1维度表
        //优化2：实现异步I/O操作
        //将异步 I/O 操作应用于 DataStream 作为 DataStream 的一次转换操作, 启用或者不启用重试，这里没有启用
        SingleOutputStreamOperator<TradeSkuOrderBean> sku = AsyncDataStream.unorderedWait(
                reduce,
                //实现分发请求的AsyncFunction
                new DimAsyncFunction<TradeSkuOrderBean>("dim_sku_info") {
                    @Override
                    public void join(TradeSkuOrderBean input, JSONObject dimJSONObj) {
                        //将相关属性赋值给流中的对象，hbase中的字段名称为大写！！！
                        input.setSkuName(dimJSONObj.getString("SKU_NAME"));
                        input.setSpuId(dimJSONObj.getString("SPU_ID"));
                        input.setTrademarkId(dimJSONObj.getString("TM_ID"));
                        input.setCategory3Id(dimJSONObj.getString("CATEGORY3_ID"));
                    }

                    @Override
                    public String getKey(TradeSkuOrderBean input) {
                        return input.getSkuId();
                    }
                },
                60,
                TimeUnit.SECONDS
        );

        SingleOutputStreamOperator<TradeSkuOrderBean> spu = AsyncDataStream.unorderedWait(
                sku,
                //实现分发请求的AsyncFunction
                new DimAsyncFunction<TradeSkuOrderBean>("dim_spu_info") {
                    @Override
                    public void join(TradeSkuOrderBean input, JSONObject dimJSONObj) {
                        //将相关属性赋值给流中的对象，hbase中的字段名称为大写！！！
                        input.setSpuName(dimJSONObj.getString("SPU_NAME"));
                    }

                    @Override
                    public String getKey(TradeSkuOrderBean input) {
                        return input.getSpuId();
                    }
                },
                60,
                TimeUnit.SECONDS
        );

        SingleOutputStreamOperator<TradeSkuOrderBean> trademark = AsyncDataStream.unorderedWait(
                spu,
                //实现分发请求的AsyncFunction
                new DimAsyncFunction<TradeSkuOrderBean>("dim_base_trademark") {
                    @Override
                    public void join(TradeSkuOrderBean input, JSONObject dimJSONObj) {
                        //将相关属性赋值给流中的对象，hbase中的字段名称为大写！！！
                        input.setTrademarkName(dimJSONObj.getString("TM_NAME"));
                    }

                    @Override
                    public String getKey(TradeSkuOrderBean input) {
                        return input.getTrademarkId();
                    }
                },
                60,
                TimeUnit.SECONDS
        );

        SingleOutputStreamOperator<TradeSkuOrderBean> category3 = AsyncDataStream.unorderedWait(
                trademark,
                //实现分发请求的AsyncFunction
                new DimAsyncFunction<TradeSkuOrderBean>("dim_base_category3") {
                    @Override
                    public void join(TradeSkuOrderBean input, JSONObject dimJSONObj) {
                        //将相关属性赋值给流中的对象，hbase中的字段名称为大写！！！
                        input.setCategory3Name(dimJSONObj.getString("NAME"));
                        input.setCategory2Id(dimJSONObj.getString("CATEGORY2_ID"));
                    }

                    @Override
                    public String getKey(TradeSkuOrderBean input) {
                        return input.getCategory3Id();
                    }
                },
                60,
                TimeUnit.SECONDS
        );

        SingleOutputStreamOperator<TradeSkuOrderBean> category2 = AsyncDataStream.unorderedWait(
                category3,
                //实现分发请求的AsyncFunction
                new DimAsyncFunction<TradeSkuOrderBean>("dim_base_category2") {
                    @Override
                    public void join(TradeSkuOrderBean input, JSONObject dimJSONObj) {
                        //将相关属性赋值给流中的对象，hbase中的字段名称为大写！！！
                        input.setCategory2Name(dimJSONObj.getString("NAME"));
                        input.setCategory1Id(dimJSONObj.getString("CATEGORY1_ID"));
                    }

                    @Override
                    public String getKey(TradeSkuOrderBean input) {
                        return input.getCategory2Id();
                    }
                },
                60,
                TimeUnit.SECONDS
        );

        SingleOutputStreamOperator<TradeSkuOrderBean> category1 = AsyncDataStream.unorderedWait(
                category2,
                //实现分发请求的AsyncFunction
                new DimAsyncFunction<TradeSkuOrderBean>("dim_base_category1") {
                    @Override
                    public void join(TradeSkuOrderBean input, JSONObject dimJSONObj) {
                        //将相关属性赋值给流中的对象，hbase中的字段名称为大写！！！
                        input.setCategory1Name(dimJSONObj.getString("NAME"));
                    }

                    @Override
                    public String getKey(TradeSkuOrderBean input) {
                        return input.getCategory1Id();
                    }
                },
                60,
                TimeUnit.SECONDS
        );

        //todo 输出到CK
        category1.print();
        category1.addSink(MyClickhouseUtil.getSinkFunction("insert into dws_trade_sku_order_window values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"));

        //启动程序执行
        env.execute();
    }
}
