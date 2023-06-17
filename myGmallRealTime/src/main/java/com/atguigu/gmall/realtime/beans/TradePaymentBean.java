package com.atguigu.gmall.realtime.beans;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * 自定义实体类
 */
@Data
@AllArgsConstructor
public class TradePaymentBean {
    // 窗口起始时间
    String stt;
    // 窗口终止时间
    String edt;
    // 支付成功独立用户数
    Long paymentSucUniqueUserCount;
    // 支付成功新用户数
    Long paymentSucNewUserCount;
    // 时间戳
    Long ts;
}
