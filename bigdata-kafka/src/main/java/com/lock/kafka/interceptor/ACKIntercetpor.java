package com.lock.kafka.interceptor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

public class ACKIntercetpor implements ProducerInterceptor{

    private int successCount = 0;
    private int errorCount = 0;

    public ProducerRecord onSend(ProducerRecord record) {
        return record;
    }

    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {

        if ( exception == null ) {
            successCount = successCount + 1;
        } else {
            errorCount = errorCount + 1;
        }
    }

    public void close() {

        System.out.println("发送成功的消息数量 = " + successCount);
        System.out.println("发送失败的消息数量 = " + errorCount);
    }

    public void configure(Map<String, ?> configs) {

    }
}
