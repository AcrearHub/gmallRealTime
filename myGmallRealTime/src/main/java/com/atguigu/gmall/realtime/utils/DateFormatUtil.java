package com.atguigu.gmall.realtime.utils;

import java.text.SimpleDateFormat;

/**
 * 日期转换工具类：毫秒数 -> 字符串
 * 注意：SimpleDateFormat有线程安全问题，建议用jdk1.8日期包下的类进行封装
 */
public class DateFormatUtil {
    private static SimpleDateFormat simpleDateFormat = new SimpleDateFormat();
    public static String toDate(Long ts){
        return simpleDateFormat.format(ts);
    }
}
