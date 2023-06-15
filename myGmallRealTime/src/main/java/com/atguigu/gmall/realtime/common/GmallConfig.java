package com.atguigu.gmall.realtime.common;

/**
 * 常量类
 */
public class GmallConfig {
    public static final String PHOENIX_SCHEMA = "GMALL_REALTIME";
    public static final String PHOENIX_DRIVER = "org.apache.phoenix.jdbc.PhoenixDriver";
    public static final String PHOENIX_URL = "jdbc:phoenix:hadoop102,hadoop103,hadoop104:2181";
    public static final String CLICKHOUSE_DRIVER = "ru.yandex.clickhouse.ClickHouseDriver";
    public static final String CLICKHOUSE_URL = "jdbc:clickhouse://hadoop102:8123/default";
}
