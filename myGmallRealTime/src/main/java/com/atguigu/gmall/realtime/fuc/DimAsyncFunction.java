package com.atguigu.gmall.realtime.fuc;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.beans.DimJoinFunction;
import com.atguigu.gmall.realtime.utils.DIMUtil;
import com.atguigu.gmall.realtime.utils.ThreadPoolUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.util.Collections;
import java.util.concurrent.ExecutorService;

/**
 * 发送异步IO进行关联
 * 采用模板方法设计模式：
 *      在父类中定义完成某一个功能的核心算法的骨架(步骤),将具体的实现延迟到子类中去完成。
 *      在不改变父类核心算法骨架的前提下，每一个子类都可以有自己不同的实现。
 */
public abstract class DimAsyncFunction<T> extends RichAsyncFunction<T,T> implements DimJoinFunction<T> {

    private String tableName;

    public DimAsyncFunction(String tableName) {
        this.tableName = tableName;
    }

    //这里不用ThreadPoolExecutor，降低和工具类的耦合度（多态），防止当ThreadPool发生更改，这里也得改
    private ExecutorService instance;

    @Override
    public void open(Configuration parameters) throws Exception {
        //获取异步线程
        instance = ThreadPoolUtil.getInstance();
    }

    @Override
    public void asyncInvoke(T input, ResultFuture<T> resultFuture) throws Exception {

        instance.submit(
                //每个线程的具体操作
                new Runnable() {
                    @Override
                    public void run() {
                        //获取要关联维度的主键
                        String key = getKey(input);
                        //获取维度对象
                        //优化1：添加旁路缓存redis
                        JSONObject dimJSONObj = DIMUtil.getDIMInfoId(tableName,key);
                        if (dimJSONObj != null) {
                            join(input,dimJSONObj);
                        }
                        //获取数据库交互的结果并发送给 ResultFuture 的回调函数
                        resultFuture.complete(Collections.singleton(input));
                    }
                }
        );
    }
}
