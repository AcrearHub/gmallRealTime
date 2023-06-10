package com.atguigu.gmall.realtime.fuc;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.beans.TableProcess;
import com.atguigu.gmall.realtime.common.GmallConfig;
import com.atguigu.gmall.realtime.utils.PhoenixUtil;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.*;
import java.util.*;

/**
 * 对广播流和主流进行一些操作
 */
public class TableProcessFunction extends BroadcastProcessFunction<JSONObject,String,JSONObject> {
    private Map<String,TableProcess> configMap = new HashMap<>();
    private MapStateDescriptor<String, TableProcess> mapStateDescriptor;
    public TableProcessFunction(MapStateDescriptor<String, TableProcess> mapStateDescriptor) {
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
        String sql = "select * from gmall_realtime_config.table_process where sink_type='dim'";
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
            TableProcess tableProcess = jsonObj.toJavaObject(TableProcess.class);
            configMap.put(tableProcess.getSourceTable(),tableProcess);
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
        //获取广播状态
        ReadOnlyBroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
        //根据表名到广播中获取对应的信息
        TableProcess tableProcess = null;
        //信息不为空，传递到下游
        if ((tableProcess = broadcastState.get(table)) != null || (tableProcess = configMap.get(table)) != null){
            //拿到data中的数据
            JSONObject data = value.getJSONObject("data");
            //移除data中不需要的信息
            String sinkColumns = tableProcess.getSinkColumns();
            filterColumn(data,sinkColumns);
            //补充sink_table（注意和上面的顺序，防止补充的数据被删除）
            String sinkTable = tableProcess.getSinkTable();
            data.put("sink_table",sinkTable);
            //传递数据
            out.collect(data);
        }
    }

    private void filterColumn(JSONObject value, String sinkColumns) {
        String[] split = sinkColumns.split(",");
        List<String> strings = Arrays.asList(split);
        Set<Map.Entry<String, Object>> entrySet = value.entrySet();
        //将JSONObject中每个键值对遍历出来，用迭代器，不能用while！！！会死循环
       /* while (entrySet.iterator().hasNext()){
            Map.Entry<String, Object> next = entrySet.iterator().next();
            if (!strings.contains(next.getKey())){
                entrySet.iterator().remove();
            }
        }*/
        entrySet.removeIf(entry -> !strings.contains(entry.getKey()));
    }

    @Override
    public void processBroadcastElement(String value, BroadcastProcessFunction<JSONObject, String, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
        //todo 处理配置（广播）流
        JSONObject jsonObject = JSON.parseObject(value);
        String op = jsonObject.getString("op");
        BroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
        //若对配置表进行删除操作，要将对应的配置信息从广播中删除
        if ("d".equals(op)){
            TableProcess before = jsonObject.getObject("before", TableProcess.class);
            String sinkType = before.getSinkType();
            //判断是否是dim的表
            if ("dim".equals(sinkType)){
                broadcastState.remove(before.getSourceTable());
                configMap.remove(before.getSourceTable());
            }
        //若对配置表进行删除之外的操作，要将对应的配置信息添加/更新到广播中
        }else {
            TableProcess after = jsonObject.getObject("after", TableProcess.class);
            String sinkType = after.getSinkType();
            if ("dim".equals(sinkType)){
                //提前将hbase的表建好
                String sinkTable = after.getSinkTable();
                String sinkColumns = after.getSinkColumns();
                String sinkPk = after.getSinkPk();
                String sinkExtend = after.getSinkExtend();
                checkTable(sinkTable,sinkColumns,sinkPk,sinkExtend);
                broadcastState.put(after.getSourceTable(),after);
                configMap.put(after.getSinkTable(),after);
            }
        }
    }

    private void checkTable(String sinkTable, String sinkColumns, String sinkPk, String sinkExtend) {
        //todo 事先完成建表操作
        //对一些null值进行处理
        if (sinkPk == null){
            sinkPk = "id";
        }
        if (sinkExtend == null){
            sinkExtend = "";
        }

        //拼接建表语句
        StringBuilder createSql = new StringBuilder("create table if not exists " + GmallConfig.PHOENIX_SCHEMA + "." + sinkTable + "(");
        String[] split = sinkColumns.split(",");
        for (int i = 0; i < split.length; i++) {
            String s = split[i];
            if (s.equals(sinkPk)){
                createSql.append(s).append(" varchar primary key ");
            }else{
                createSql.append(s).append(" varchar ");
            }
            if (i < split.length - 1){
                createSql.append(" , ");
            }
        }
        createSql.append(" ) ").append(sinkExtend);
        //写出建表语句
        System.out.println("建表语句：" + createSql);

        //执行语句（JDBC）
        PhoenixUtil.executeSql(createSql.toString());
    }
}
