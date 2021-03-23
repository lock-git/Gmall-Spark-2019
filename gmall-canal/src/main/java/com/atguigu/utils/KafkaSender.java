package com.atguigu.utils;

import com.atguigu.constants.GmallConstants;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class KafkaSender {

    private static KafkaProducer<String, String> kafkaProducer;

    //获取Producer
    static {
        //读取配置文件
        Properties properties = PropertiesUtil.load("kafka.producer.properties");

        kafkaProducer = new KafkaProducer<>(properties);
    }

    //发送数据
    public static void sendCanalData(String topic, String data) {
        kafkaProducer.send(new ProducerRecord<>(topic, data));
    }
}
