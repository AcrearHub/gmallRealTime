package com.atguigu.gmall.realtime.utils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.GmallConfig;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.tuple.Tuple2;
import redis.clients.jedis.Jedis;

import java.util.List;

/**
 * 查询DIM层的类，调用PhoenixUtil的queryList方法，返回JSONObject的一条数据（专用版本的queryList）
 */
public class DIMUtil {
    /**
     * 无优化：将可变参数封装为二元组，可有多个查询条件
     */
    public static JSONObject getDIMInfoNoCache(String tableName, Tuple2<String,String>... columnNameAndValues) {
        //拼接sql
        StringBuilder sql = new StringBuilder("select * from " + GmallConfig.PHOENIX_SCHEMA + "." + tableName + " where ");
        for (int i = 0; i < columnNameAndValues.length; i++) {
            Tuple2<String, String> columnNameAndValue = columnNameAndValues[i];
            String columnName = columnNameAndValue.f0;
            String columnValue = columnNameAndValue.f1;
            sql.append(columnName).append("= '").append(columnValue).append("'");
            if (i < columnNameAndValues.length - 1){
                sql.append(" and ");
            }
        }

        List<JSONObject> jsonObjectList = PhoenixUtil.queryList(sql.toString(), JSONObject.class);
        JSONObject jsonObject = null;
        if (jsonObjectList.size() > 0){
            //集合中只有一条元素
            jsonObject = jsonObjectList.get(0);
        } else {
            //没有对应数据
            System.out.println("hbase中没有对应维度数据");
        }
        return jsonObject;
    }

    /**
     * 优化1：添加旁路缓存redis：性能尚可，维护性好
     *  key：dim:维度表名:主键1_主键2_...（表名小写）
     *  type：string
     *  缓存时间：1day
     *  若dim表发生变化，需删除redis中数据
     */
    public static JSONObject getDIMInfo(String tableName, Tuple2<String,String>... columnNameAndValues) {
        //拼接从Redis中查询维度的key
        StringBuilder redisKey = new StringBuilder("dim:" + tableName.toLowerCase() + ":");
        //拼接sql
        StringBuilder sql = new StringBuilder("select * from " + GmallConfig.PHOENIX_SCHEMA + "." + tableName + " where ");
        for (int i = 0; i < columnNameAndValues.length; i++) {
            Tuple2<String, String> columnNameAndValue = columnNameAndValues[i];
            String columnName = columnNameAndValue.f0;
            String columnValue = columnNameAndValue.f1;
            sql.append(columnName).append("= '").append(columnValue).append("'");
            redisKey.append(columnValue);
            if (i < columnNameAndValues.length - 1){
                sql.append(" and ");
                redisKey.append("_");
            }
        }

        //创建Jedis客户端
        Jedis jedis = null; //K
        String jedisStr = null; //V
        JSONObject dimJSONObject = null;    //V的具体值

        try {
            jedis = RedisUtil.getJedis();
            jedisStr = jedis.get(redisKey.toString());
            if (StringUtils.isNotEmpty(jedisStr)){
                //缓存中有数据，从缓存中读
                dimJSONObject = JSON.parseObject(jedisStr);
            } else {
                //缓存中没有数据，从hbase中读，并缓存到redis
                List<JSONObject> jsonObjectList = PhoenixUtil.queryList(sql.toString(), JSONObject.class);
                if (jsonObjectList.size() > 0){
                    //集合中只有一条元素
                    dimJSONObject = jsonObjectList.get(0);
                    jedis.setex(redisKey.toString(), 3600 * 24, dimJSONObject.toJSONString());
                } else {
                    //没有对应数据
                    System.out.println("hbase中没有对应维度数据");
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (jedis != null){
                //关闭Jedis客户端
                jedis.close();
            }
        }
        return dimJSONObject;
    }

    /**
     * 封装上面方法，只对id进行筛选
     */
    public static JSONObject getDIMInfoId(String tableName, String id){
        return getDIMInfo(tableName,Tuple2.of("id",id));
    }

    /**
     * 删除Redis缓存
     */
    public static void deleteCache(String tableName, String id) {
        //拼接从Redis中查询维度的key
        StringBuilder redisKey = new StringBuilder("dim:" + tableName.toLowerCase() + ":");
        Jedis jedis = null;
        try {
            jedis = RedisUtil.getJedis();
            jedis.del(redisKey.toString());
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (jedis != null){
                System.out.println("删除缓存后，关闭Jedis客户端");
                jedis.close();
            }
        }
    }
}
