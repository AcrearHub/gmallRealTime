package com.atguigu.gmall.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.beans.TradeTrademarkCategoryUserRefundBean;
import com.atguigu.gmall.realtime.fuc.DimAsyncFunction;
import com.atguigu.gmall.realtime.utils.DateFormatUtil;
import com.atguigu.gmall.realtime.utils.MyClickhouseUtil;
import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.Collections;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;

/**
 * DWS：交易域品牌-品类-用户粒度退单各窗口汇总表
 */
public class DWSTradeTrademarkCategoryUserRefundWindow {
    public static void main(String[] args) throws Exception {
        //todo 创建流式处理
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置全局并行度：不设置默认为全并行度；1为单线程执行
        env.setParallelism(4);
        //检查点相关设置（略）
        //todo 从Kafka中读取订单明细表数据
        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> map = env
                .fromSource(MyKafkaUtil.getKafkaSource("topic_dwd_trade_order_detail", "dws_trade_trademark_category_user_refund_window"), WatermarkStrategy.noWatermarks(), "kafka_source")
                //todo 对数据进行类型转换jsonStr -> jsonObj，由于不统计金额之类，只需去重退单数量即可
                .map(JSON::parseObject)
                //todo 封装为实体类对象，去重退单数——Set集合方法
                .map(
                        new MapFunction<JSONObject, TradeTrademarkCategoryUserRefundBean>() {
                            @Override
                            public TradeTrademarkCategoryUserRefundBean map(JSONObject value) {
                                return TradeTrademarkCategoryUserRefundBean.builder()
                                        .userId(value.getString("user_id"))
                                        .skuId(value.getString("sku_id"))
                                        .orderIdSet(new HashSet<>(Collections.singleton(value.getString("order_id"))))
                                        .ts(value.getLong("ts") * 1000)
                                        .build();
                            }
                        }
                );
        //todo 关联sku维度表，获取tm_id、category3_id
        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> sku = AsyncDataStream.unorderedWait(
                map,
                new DimAsyncFunction<TradeTrademarkCategoryUserRefundBean>("dim_sku_info") {
                    @Override
                    public void join(TradeTrademarkCategoryUserRefundBean input, JSONObject dimJSONObj) {
                        input.setTrademarkId(dimJSONObj.getString("TM_ID"));
                        input.setCategory3Id(dimJSONObj.getString("CATEGORY3_ID"));
                    }

                    @Override
                    public String getKey(TradeTrademarkCategoryUserRefundBean input) {
                        return input.getSkuId();
                    }
                },
                60L, TimeUnit.SECONDS
        );
        //todo 设置Watermark
        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> reduce = sku
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<TradeTrademarkCategoryUserRefundBean>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(
                                (SerializableTimestampAssigner<TradeTrademarkCategoryUserRefundBean>) (element, recordTimestamp) -> element.getTs())
                )
                //todo 按照品牌、品类、用户分组、开窗、聚合
                .keyBy(
                        new KeySelector<TradeTrademarkCategoryUserRefundBean, String>() {
                            @Override
                            public String getKey(TradeTrademarkCategoryUserRefundBean value) {
                                return value.getTrademarkId() + ":" + value.getCategory3Id() + ":" + value.getUserId();
                            }
                        }
                )
                .window(TumblingEventTimeWindows.of(Time.seconds(10L)))
                .reduce(
                        new ReduceFunction<TradeTrademarkCategoryUserRefundBean>() {
                            @Override
                            public TradeTrademarkCategoryUserRefundBean reduce(TradeTrademarkCategoryUserRefundBean value1, TradeTrademarkCategoryUserRefundBean value2) {
                                value1.getOrderIdSet().addAll(value2.getOrderIdSet());
                                return value1;
                            }
                        },
                        new WindowFunction<TradeTrademarkCategoryUserRefundBean, TradeTrademarkCategoryUserRefundBean, String, TimeWindow>() {
                            @Override
                            public void apply(String s, TimeWindow window, Iterable<TradeTrademarkCategoryUserRefundBean> input, Collector<TradeTrademarkCategoryUserRefundBean> out) {
                                String stt = DateFormatUtil.toYmdHms(window.getStart());
                                String edt = DateFormatUtil.toYmdHms(window.getEnd());
                                for (TradeTrademarkCategoryUserRefundBean element : input) {
                                    element.setStt(stt);
                                    element.setEdt(edt);
                                    element.setRefundCount((long) element.getOrderIdSet().size());
                                    element.setTs(System.currentTimeMillis());
                                    out.collect(element);
                                }
                            }
                        }
                );

        //todo 从hbase中其他关联维度表
        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> trademark = AsyncDataStream.unorderedWait(
                reduce,
                //实现分发请求的AsyncFunction
                new DimAsyncFunction<TradeTrademarkCategoryUserRefundBean>("dim_base_trademark") {
                    @Override
                    public void join(TradeTrademarkCategoryUserRefundBean input, JSONObject dimJSONObj) {
                        //将相关属性赋值给流中的对象，hbase中的字段名称为大写！！！
                        input.setTrademarkName(dimJSONObj.getString("TM_NAME"));
                    }

                    @Override
                    public String getKey(TradeTrademarkCategoryUserRefundBean input) {
                        return input.getTrademarkId();
                    }
                },
                60,
                TimeUnit.SECONDS
        );

        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> category3 = AsyncDataStream.unorderedWait(
                trademark,
                //实现分发请求的AsyncFunction
                new DimAsyncFunction<TradeTrademarkCategoryUserRefundBean>("dim_base_category3") {
                    @Override
                    public void join(TradeTrademarkCategoryUserRefundBean input, JSONObject dimJSONObj) {
                        //将相关属性赋值给流中的对象，hbase中的字段名称为大写！！！
                        input.setCategory3Name(dimJSONObj.getString("NAME"));
                        input.setCategory2Id(dimJSONObj.getString("CATEGORY2_ID"));
                    }

                    @Override
                    public String getKey(TradeTrademarkCategoryUserRefundBean input) {
                        return input.getCategory3Id();
                    }
                },
                60,
                TimeUnit.SECONDS
        );

        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> category2 = AsyncDataStream.unorderedWait(
                category3,
                //实现分发请求的AsyncFunction
                new DimAsyncFunction<TradeTrademarkCategoryUserRefundBean>("dim_base_category2") {
                    @Override
                    public void join(TradeTrademarkCategoryUserRefundBean input, JSONObject dimJSONObj) {
                        //将相关属性赋值给流中的对象，hbase中的字段名称为大写！！！
                        input.setCategory2Name(dimJSONObj.getString("NAME"));
                        input.setCategory1Id(dimJSONObj.getString("CATEGORY1_ID"));
                    }

                    @Override
                    public String getKey(TradeTrademarkCategoryUserRefundBean input) {
                        return input.getCategory2Id();
                    }
                },
                60,
                TimeUnit.SECONDS
        );

        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> category1 = AsyncDataStream.unorderedWait(
                category2,
                //实现分发请求的AsyncFunction
                new DimAsyncFunction<TradeTrademarkCategoryUserRefundBean>("dim_base_category1") {
                    @Override
                    public void join(TradeTrademarkCategoryUserRefundBean input, JSONObject dimJSONObj) {
                        //将相关属性赋值给流中的对象，hbase中的字段名称为大写！！！
                        input.setCategory1Name(dimJSONObj.getString("NAME"));
                    }

                    @Override
                    public String getKey(TradeTrademarkCategoryUserRefundBean input) {
                        return input.getCategory1Id();
                    }
                },
                60,
                TimeUnit.SECONDS
        );

        //todo 输出到CK
        category1.print();
        category1.addSink(MyClickhouseUtil.getSinkFunction("insert into dws_trade_trademark_category_user_refund_window values(?,?,?,?,?,?,?,?,?,?,?,?,?)"));

        //启动程序执行
        env.execute();
    }
}
