package com.atguigu.gmall.realtime.utils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * 通过Phoenix执行sql语句
 */
public class PhoenixUtil {
    public static void executeSql(String sql){
        //执行语句（JDBC）
        Connection connection = null;
        PreparedStatement preparedStatement = null;
        try {
            //注册驱动、获取连接（已在Druid中配置）
            //设置schema到namespace的映射
            connection = DruidDSUtil.getConnection();
            //编译SQL
            preparedStatement = connection.prepareStatement(sql);
            //执行SQL
            preparedStatement.execute();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            //关闭连接
            if (preparedStatement != null) {
                try {
                    preparedStatement.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (connection != null){
                try {
                    connection.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
