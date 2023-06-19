package com.atguigu.gmall.realtime.beans;

import com.alibaba.fastjson.JSONObject;

/**
 * 维度关联实现的方法
 */
public interface DimJoinFunction<T> {
    void join(T input, JSONObject dimJSONObj);

    String getKey(T input);
}
