package com.atguigu.gmall.realtime.fuc;

import com.atguigu.gmall.realtime.utils.KeyWordUtil;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

/**
 * 自定义UDTF：由于使用的是FlinkSql，无法调用工具类，只能将其封装到自定义函数
 */
@FunctionHint(output = @DataTypeHint("ROW<word STRING>"))   //表示Row这一行中都有什么列，这里只有word一列
public class KeyWordUDTF extends TableFunction<Row> {
    public void eval(String text) {
        for (String keyword : KeyWordUtil.analyze(text)) {
            collect(Row.of(keyword));
        }
    }
}
