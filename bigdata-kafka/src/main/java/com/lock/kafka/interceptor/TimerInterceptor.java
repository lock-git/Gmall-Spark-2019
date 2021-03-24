package com.lock.kafka.interceptor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

public class TimerInterceptor implements ProducerInterceptor {
    /**
     * 在发送数据之前需要进行的逻辑操作
     * @param record
     * @return
     */
    public ProducerRecord onSend(ProducerRecord record) {
        String newValue = System.currentTimeMillis() + "-" +record.value();
        return new ProducerRecord(record.topic(), record.partition(), record.timestamp(), record.key(), newValue);
    }

    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {

    }

    public void close() {

    }

    public void configure(Map<String, ?> configs) {

    }
}
