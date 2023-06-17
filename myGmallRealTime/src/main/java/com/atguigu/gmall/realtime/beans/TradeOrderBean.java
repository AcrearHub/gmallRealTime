package com.atguigu.gmall.realtime.beans;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * 自定义类
 */
@Data
@AllArgsConstructor
public class TradeOrderBean {
    // 窗口起始时间
    String stt;
    // 窗口关闭时间
    String edt;
    // 下单独立用户数
    Long orderUniqueUserCount;
    // 下单新用户数
    Long orderNewUserCount;
    // 时间戳
    Long ts;
}
