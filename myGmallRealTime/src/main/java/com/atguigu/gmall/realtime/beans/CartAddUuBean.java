package com.atguigu.gmall.realtime.beans;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * 自定义类
 */
@Data
@AllArgsConstructor
public class CartAddUuBean {
    // 窗口起始时间
    String stt;
    // 窗口闭合时间
    String edt;
    // 加购独立用户数
    Long cartAddUuCt;
    // 时间戳
    Long ts;

}
