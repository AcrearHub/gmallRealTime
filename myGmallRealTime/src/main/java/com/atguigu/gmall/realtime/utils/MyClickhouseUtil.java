package com.atguigu.gmall.realtime.utils;

import com.atguigu.gmall.realtime.beans.TransientSink;
import com.atguigu.gmall.realtime.common.GmallConfig;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.lang.reflect.Field;

/**
 * Clickhouse工具类
 */
public class MyClickhouseUtil {
    public static <T>SinkFunction<T> getSinkFunction(String sql){
        return JdbcSink.sink(
                sql,
                (JdbcStatementBuilder<T>) (preparedStatement, t) -> {
                    //通过反射拿到类的相关属性，封装为Field数组（数组和属性顺序对应；若错位，自己想办法）
                    Field[] declaredFields = t.getClass().getDeclaredFields();
                    int skipNum = 0;
                    for (int i = 0; i < declaredFields.length; i++) {
                        Field declaredField = declaredFields[i];
                        //判断当前属性是否需要保存到CK中（通过自定义注解来标识）
                        TransientSink annotation = declaredField.getAnnotation(TransientSink.class);
                        if (annotation != null){
                            skipNum++;  //跳过时需要计数，防止在赋值到占位符时下标错位
                            continue;
                        }
                        //设置private属性的访问权限
                        declaredField.setAccessible(true);
                        try {
                            //拿到属性对应的值
                            Object value = declaredField.get(t);
                            //赋值给？
                            preparedStatement.setObject(i + 1 - skipNum,value);   //占位符从1开始，这里的循环从0开始
                        } catch (IllegalAccessException e) {
                            e.printStackTrace();
                        }
                    }
                },
                new JdbcExecutionOptions
                        .Builder()
                        .withBatchSize(5)    //设置单个并行度的批次操作，默认5000，根据实际情况调整
                        .withBatchIntervalMs(3000)  //设置每次攒批的最长时间
                        .build()
                ,
                new JdbcConnectionOptions
                        .JdbcConnectionOptionsBuilder()
                        .withDriverName(GmallConfig.CLICKHOUSE_DRIVER)
                        .withUrl(GmallConfig.CLICKHOUSE_URL)
                        .build()
        );
    }
}
