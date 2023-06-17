package com.atguigu.gmall.realtime.app.dws;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

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
        //todo 从Kafka中读取4张表的数据
        //todo 过滤空消息，对数据进行类型转换jsonStr -> jsonObj
        //todo 去重消息
        //todo 封装为实体类对象
        //todo 设置Watermark
        //todo 按照sku分组、开窗、聚合
        //todo 关联商品sku、spu、品牌、类别3、类别2维度
        //todo 输出到CK

        //启动程序执行
        env.execute();
    }
}
