package com.atguigu.gmall.realtime.beans;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * 自定义注解，标记不需要往CK中保存的属性
 */
@Target(ElementType.FIELD)   //标记注解，用来指定本注解加到什么位置（类、属性、方法、参数等）
@Retention(RetentionPolicy.RUNTIME)    //注解生效范围，这里使用反射可读
public @interface TransientSink {
}
