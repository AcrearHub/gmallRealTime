package com.atguigu.gmall.realtime.utils;

import org.apache.commons.beanutils.BeanUtils;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * 通过Phoenix执行sql语句
 */
public class PhoenixUtil {
    public static void executeSql(String sql) {
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

    public static <T>List<T> queryList(String sql, Class<T> tClass) {
        //JDBC连接
        List<T> arrList = new ArrayList<>();
        Connection connection = null;
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;
        try {
            connection = DruidDSUtil.getConnection();
            preparedStatement = connection.prepareStatement(sql);
            resultSet = preparedStatement.executeQuery();
            ResultSetMetaData metaData = resultSet.getMetaData();
            while (resultSet.next()){
                T t = tClass.newInstance();
                for (int i = 1; i <= metaData.getColumnCount(); i++) {
                    String columnName = metaData.getColumnName(i);
                    Object columnValue = resultSet.getObject(i);
                    //不能用反射，因为这里不知道T的类型。BeanUtils将所有类型都考虑进去了
                    BeanUtils.setProperty(t,columnName,columnValue);
                }
                arrList.add(t);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (resultSet != null) {
                try {
                    resultSet.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
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
        return arrList;
    }
}
