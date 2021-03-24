package com.lock.kafka.producer;

import org.apache.kafka.clients.producer.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class CustomProducer {
    public static void main(String[] args) throws Exception {

        // 创建配置对象
     Properties props = new Properties();
     // kafka集群配置
     props.put("bootstrap.servers", "linux1:9092");
     // kafka ACK应答机制
    // 0 ：不需要应答
    // 1 ：需要Leader应答
    // -1（all）: 需要所有的副本全部应答
     props.put("acks", "all");
     // 重试
     props.put("retries", 0);
     // batch大小
     props.put("batch.size", 16384);
     // 延迟提交
     props.put("linger.ms", 1);
     // 缓存
     props.put("buffer.memory", 33554432);
     // 序列化
     props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
     props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
     props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, "com.lock.kafka.partitioner.CustomPartitioner");

        List<String> interceptors = new ArrayList<String>();
        interceptors.add("com.lock.kafka.interceptor.TimerInterceptor");
        interceptors.add("com.lock.kafka..kafka.interceptor.ACKIntercetpor");

        props.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, interceptors);

     // 构建生产者对象
     // 使用Kafka JavaAPI
     Producer<String, String> producer = new KafkaProducer<String, String>(props);
     // kafka会将数据转换为ProducerRecord对象然后发送
     // 00000000.log
     // message : offset, size, message
     for (int i = 0; i < 10; i++) {
         // 封装数据
         ProducerRecord<String, String> record = new ProducerRecord<String, String>("first", 0, Integer.toString(i), Integer.toString(i));

         // 发送数据
         // 同步发送
         //producer.send(record).get();
         // 异步发送
         producer.send(record, new Callback() {
             public void onCompletion(RecordMetadata metadata, Exception exception) {
                 System.out.println(metadata.topic() + "," + metadata.partition() + "," + metadata.offset());
             }
         });

     }

     // 资源关闭
     producer.close();

    }
}
