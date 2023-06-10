package com.atguigu.gmall.realtime.utils;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

/**
 * Kafka的输入、输出方法
 */
public class MyKafkaUtil {
    private static final String KAFKA_SERVER = "hadoop102:9092,hadoop03:9092,hadoop104:9092";
    public static KafkaSource<String> getKafkaSource(String topic,String groupId){
        return KafkaSource
                .<String>builder()
                .setBootstrapServers(KAFKA_SERVER)
                .setTopics(topic)
                .setGroupId(groupId)
//              .setDeserializer()  //针对kv的反序列化器设置，需要如下自定义：对null值处理
                .setValueOnlyDeserializer(new DeserializationSchema<String>() {
                    @Override
                    public String deserialize(byte[] message) {
                        if (message != null){
                            return new String(message);
                        }
                        return null;
                    }

                    @Override
                    public boolean isEndOfStream(String nextElement) {
                        return false;
                    }

                    @Override
                    public TypeInformation<String> getProducedType() {
                        return TypeInformation.of(String.class);
                    }
                })  //针对v的反序列化器设置，需自定义，原有反序列化器无法处理空值
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.LATEST))    //offset重置策略
                //如果是精确一次，则必须设置2PC读已提交：👇
//              .setProperty(ConsumerConfig.ISOLATION_LEVEL_CONFIG,"read_committed")
                .build();
    }

    public static KafkaSink<String> getKafkaSink(String topic /*, String transactionalIdPrefix */){
        //将脏数据写入Kafka
        return KafkaSink
                .<String>builder()
                .setBootstrapServers(KAFKA_SERVER)
                .setRecordSerializer(
                        KafkaRecordSerializationSchema
                                .builder()
                                .setTopic(topic) //设置主题
                                .setValueSerializationSchema(new SimpleStringSchema())  //设置序列化
                                .build()
                )
                //精确一次👇（测试时无法做到，检查点可能会卡死，除非使用优雅关闭）
//              .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                //如果是精确一次，则必须设事务超时时间（默认1h）小于检查点超时时间（默认15min）：👇
//              .setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, 15 * 60 * 1000 + "")
                //如果是精确一次，则必须设置事务ID：👇
//              .setTransactionalIdPrefix(transactionalIdPrefix)
                .build();
    }
}
