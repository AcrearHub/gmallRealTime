package com.atguigu.gmall.realtime.app.dws;

import com.atguigu.gmall.realtime.beans.KeyWordBean;
import com.atguigu.gmall.realtime.fuc.KeyWordUDTF;
import com.atguigu.gmall.realtime.utils.MyClickhouseUtil;
import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * DWS：流量域搜索关键词粒度页面浏览各窗口汇总表
 */
public class DWSTrafficSourceKeywordPageViewWindow {
    public static void main(String[] args) throws Exception {
        //todo 创建流式处理
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置全局并行度：不设置默认为全并行度；1为单线程执行
        env.setParallelism(4);
        //检查点相关设置（略）
        //创建表执行环境
        StreamTableEnvironment streamTableEnvironment = StreamTableEnvironment.create(env);
        //注册自定义函数
        streamTableEnvironment.createTemporarySystemFunction("ik_analyze", KeyWordUDTF.class);

        //todo 从Kafka中读取DWD流量域事务事实表处理中的页面日志
        streamTableEnvironment
                .executeSql(
                        "CREATE TABLE page_log (\n" +
                                "  common map<String,string>,\n" +
                                "  page map<String,string>,\n" +
                                "  ts bigint,\n" +
                                "  row_time AS TO_TIMESTAMP(FROM_UNIXTIME(ts/1000, 'yyyy-MM-dd HH:mm:ss')),\n" +
                                "  WATERMARK FOR row_time AS row_time\n" +
                                ") " + MyKafkaUtil.getKafkaDDL("topic_page", "dws_traffic_source_keyword_page_view_window")
                );

        //todo 过滤出搜索行为，设置Watermark、Timestamp，形成表
        Table searchTable = streamTableEnvironment
                .sqlQuery(
                        "select\n" +
                                " page['item'] full_word,row_time\n" +
                                "from page_log where page['last_page_id']='search' and page['item_type']='keyword'"
                );
        streamTableEnvironment.createTemporaryView("search_table",searchTable);

        //todo 使用自定义分词函数进行分词（事先注册），并将结果和原表进行关联
        Table splitTable = streamTableEnvironment
                .sqlQuery(
                        "SELECT  keyword,row_time FROM search_table,\n" +
                                "   LATERAL TABLE(ik_analyze(full_word)) t(keyword)"
                );
        streamTableEnvironment.createTemporaryView("split_table",splitTable);

        //todo 分组、开窗、聚合计算
        Table reduceTable = streamTableEnvironment
                .sqlQuery(
                        "SELECT\n" +
                                "  DATE_FORMAT(TUMBLE_START(row_time, INTERVAL '10' SECOND),'yyyy-MM-dd HH:mm:ss') stt,\n" +
                                "  DATE_FORMAT(TUMBLE_END(row_time, INTERVAL '10' SECOND),'yyyy-MM-dd HH:mm:ss') edt,\n" +
                                "  keyword,\n" +
                                "  count(*) keyword_count,\n" +
                                "  UNIX_TIMESTAMP()*1000 ts\n" +
                                "from split_table\n" +
                                "GROUP BY\n" +
                                "  TUMBLE(row_time, INTERVAL '10' second),\n" +
                                "  keyword"
                );

        //todo 将动态表转为流
        DataStream<KeyWordBean> keyWordBeanDataStream = streamTableEnvironment.toDataStream(reduceTable, KeyWordBean.class);//仅追加，并声明转换后的类型
        keyWordBeanDataStream.print("关键词搜索次数");

        //todo 输出到CK
        keyWordBeanDataStream.addSink(MyClickhouseUtil.getSinkFunction("insert into dws_traffic_keyword_page_view_window values(?,?,?,?,?)"));

        //启动程序执行
        env.execute();
    }
}
