package com.atguigu.gmall.realtime.utils;

import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * 线程池工具类，同Druid连接池，单例
 */
public class ThreadPoolUtil {
    //volatile：提升内存可见性
    private static volatile ThreadPoolExecutor threadPoolExecutor;
    //获取线程池对象，这里采用懒汉式单例（比饿汉式复杂），由于是多线程，需要加锁synchronized
    public static ThreadPoolExecutor getInstance() {
        //双重校验锁
        //本层判断是防止若下面有不需要锁住的代码，可以顺利执行
        if (threadPoolExecutor == null){
            synchronized (ThreadPoolUtil.class){
                //本层防止多线程进锁后，重复创建对象
                if (threadPoolExecutor == null) {
                    threadPoolExecutor = new ThreadPoolExecutor(4, 20, 300L, TimeUnit.SECONDS,
                            new LinkedBlockingDeque<Runnable>(Integer.MAX_VALUE));
                }
            }
        } else {
            //其它代码
        }
        return threadPoolExecutor;
    }
}
