package com.lock.kafka.consumer;

import com.lock.kafka.consumer.helper.ConsumerHelper;
import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.cluster.BrokerEndPoint;
import kafka.javaapi.*;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.MessageAndOffset;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

public class LowApiConsumer {

    @SuppressWarnings("all")
    public static void main(String[] args) throws Exception {

        List<String> dataList = ConsumerHelper.getDatas("linux1", 9092, "first", 3, 108);

        if ( dataList == null ) {
            System.out.println("数据获取不到");
        } else {
            for (String s : dataList) {
                System.out.println(s);
            }
        }

    }

}
