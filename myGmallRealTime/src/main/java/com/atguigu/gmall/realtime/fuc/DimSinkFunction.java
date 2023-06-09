package com.atguigu.gmall.realtime.fuc;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.GmallConfig;
import com.atguigu.gmall.realtime.utils.PhoenixUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.util.Collection;
import java.util.Set;

/**
 * todo 将DIM数据写到Phoenix表中
 * 插入语句：upsert into 表空间.表名 (a,b,d) values ('aa','bb','cc')；
 */
public class DimSinkFunction implements SinkFunction<JSONObject> {
    @Override
    public void invoke(JSONObject value, Context context) {
        String sinkTable = value.getString("sink_table");
        value.remove("sink_table");
        //虽然这里的Set是无序的，但由于是从JSONObject实现的Map中拿到的数据，所以是有序的
        Set<String> keySet = value.keySet();
        Collection<Object> values = value.values();
        //拼接语句
        String upsertSql = "upsert into " + GmallConfig.PHOENIX_SCHEMA + "." + sinkTable
                + " (" + StringUtils.join(keySet, ",") + ") values('" + StringUtils.join(values, "','") + "')";
        //写出建表语句
        System.out.println("upsert语句：" + upsertSql);
        //执行语句（JDBC）
        PhoenixUtil.executeSql(upsertSql);
    }
}
