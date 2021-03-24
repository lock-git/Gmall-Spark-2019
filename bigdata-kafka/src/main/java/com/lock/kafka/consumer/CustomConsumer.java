package com.lock.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

public class CustomConsumer {
    public static void main(String[] args) {
         // 消费者的配置信息
         Properties props = new Properties();
         // kafka集群配置
         props.put("bootstrap.servers", "linux1:9092");
         // 消费者组
         props.put("group.id", "test123");
         props.put("enable.auto.commit", "true");
         props.put("auto.commit.interval.ms", "1000");
         // 反序列化
         props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
         props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

         // 创建消费者对象
         KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
         // 订阅主题
         consumer.subscribe(Arrays.asList("first"));

         //
         while (true) {
             // 读取数据
             ConsumerRecords<String, String> records = consumer.poll(100);
             for (ConsumerRecord<String, String> record : records) {
                 System.out.println(record);
             }
                 // System.out.println(record.partition() + "," + record.offset() + "," + record.value());
         }
    }
}
