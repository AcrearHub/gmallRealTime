package com.atguigu.gmall.realtime.fuc;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.beans.TableProcess;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.*;
import java.util.*;

public class TableProcessFunciont_2 extends BroadcastProcessFunction<JSONObject,String,JSONObject> {
    private MapStateDescriptor<String, TableProcess> mapStateDescriptor;
    private Map<String,TableProcess> configMap = new HashMap<>();

    public TableProcessFunciont_2(MapStateDescriptor<String, TableProcess> mapStateDescriptor) {
        this.mapStateDescriptor = mapStateDescriptor;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        //todo 从配置表中读取配置信息  将配置信息预加载到map集合中，防止出现主流先到，配置流后到的情况
        //注册驱动
        Class.forName("com.mysql.cj.jdbc.Driver");
        //建立连接
        Connection connection = DriverManager.getConnection("jdbc:mysql://hadoop102:3306/gmall_realtime_config?" +
                "user=root&password=000000&useUnicode=true&characterEncoding=utf8&serverTimeZone=Asia/Shanghai&useSSL=false");
        //获取数据库操作对象
        String sql = "select * from gmall_realtime_config.table_process where sink_type='dwd'";
        PreparedStatement prepareStatement = connection.prepareStatement(sql);
        //在resource中设置schema到namespace的映射
        //执行SQL语句
        ResultSet resultSet = prepareStatement.executeQuery();

        //获取查询结果集对应的表元数据信息
        ResultSetMetaData metaData = resultSet.getMetaData();
        //按行处理结果集
        while(resultSet.next()){
            //定义一个json对象用于接收每行字段的结果
            JSONObject jsonObj = new JSONObject();
            for (int i = 1; i <= metaData.getColumnCount(); i++) {
                String columnName = metaData.getColumnName(i);
                Object columnValue = resultSet.getObject(i);
                jsonObj.put(columnName,columnValue);
            }
            TableProcess tableProcess = JSON.toJavaObject(jsonObj,TableProcess.class);
            if (tableProcess != null){
                String sourceTable = tableProcess.getSourceTable();
                String sourceType = tableProcess.getSourceType();
                configMap.put(sourceTable + ":" + sourceType,tableProcess);
            }
        }
        //释放资源
        resultSet.close();
        prepareStatement.close();
        connection.close();
    }

    @Override
    public void processElement(JSONObject value, BroadcastProcessFunction<JSONObject, String, JSONObject>.ReadOnlyContext ctx, Collector<JSONObject> out) throws Exception {
        //todo 处理主流
        //获取业务表名
        String table = value.getString("table");
        String type = value.getString("type");
        //获取广播状态
        ReadOnlyBroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
        //根据表名到广播中获取对应的信息
        TableProcess tableProcess = null;
        //信息不为空，传递到下游
        if ((tableProcess = broadcastState.get(table + ":" + type)) != null || (tableProcess = configMap.get(table + ":" + type)) != null){
            //拿到data中的数据
            JSONObject data = value.getJSONObject("data");
            //移除data中不需要的信息
            String sinkColumns = tableProcess.getSinkColumns();
            filterColumn(data,sinkColumns);
            //补充sink_table（注意和上面的顺序，防止补充的数据被删除）
            String sinkTable = tableProcess.getSinkTable();
            data.put("sink_table",sinkTable);
            data.put("ts",value.getLong("ts"));
            //传递数据
            out.collect(data);
        }
    }

    private void filterColumn(JSONObject data, String sinkColumns) {
        String[] split = sinkColumns.split(",");
        List<String> strings = Arrays.asList(split);
        Set<Map.Entry<String, Object>> entrySet = data.entrySet();
        entrySet.removeIf(entry -> !strings.contains(entry.getKey()));
    }

    @Override
    public void processBroadcastElement(String value, BroadcastProcessFunction<JSONObject, String, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
        //todo 处理广播流
        JSONObject jsonObject = JSON.parseObject(value);
        String op = jsonObject.getString("op");
        BroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
        if ("d".equals(op)){
            //将delete信息删除
            TableProcess before = jsonObject.getObject("before", TableProcess.class);
            String sinkType = before.getSinkType();
            if ("dwd".equals(sinkType)){
                //将删除前的数据删除
                String sourceTable = before.getSourceTable();
                String sourceType = before.getSourceType();
                broadcastState.remove(sourceTable + ":" + sourceType);
                configMap.remove(sourceTable + ":" + sourceType);
            }
        } else {
            //将其余操作的信息放到广播状态中
            TableProcess after = jsonObject.getObject("after", TableProcess.class);
            String sinkType = after.getSinkType();
            if ("dwd".equals(sinkType)){
                String sourceTable = after.getSourceTable();
                String sourceType = after.getSourceType();
                broadcastState.put(sourceTable + ":" + sourceType,after);
                configMap.put(sourceTable + ":" + sourceType,after);
            }
        }
    }
}
