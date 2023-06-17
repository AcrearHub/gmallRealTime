package com.atguigu.gmall.realtime.beans;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * 自定义类
 */
@Data
@AllArgsConstructor
public class TrafficHomeDetailPageViewBean {
    // 窗口起始时间
    String stt;
    // 窗口结束时间
    String edt;
    // 首页独立访客数
    Long homeUvCt;
    // 商品详情页独立访客数
    Long goodDetailUvCt;
    // 时间戳
    Long ts;

}
