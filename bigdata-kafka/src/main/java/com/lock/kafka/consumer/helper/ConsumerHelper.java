package com.lock.kafka.consumer.helper;

import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.cluster.BrokerEndPoint;
import kafka.javaapi.*;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.MessageAndOffset;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ConsumerHelper {

    /**
     * 读取指定topic，指定partition,指定offset的数据
     *
     * 因为一个topic中存在多个partition，而且每一个partition中会有多个副本，所以想要获取指定的数据
     * 必须从指定分区的主副本中获取数据，那么就必须拿到主副本的元数据信息。
     * 1）发送主题元数据请求对象
     * 2）得到主题元数据响应对象，其中包含主题元数据信息
     * 3）通过主题元数据信息，获取主题的分区元数据信息
     * 4）获取指定的分区元数据信息
     * 5）获取分区的主副本信息
     *
     * 获取主副本信息后，消费者要连接对应的主副本机器，然后抓取数据
     * 1）构建抓取数据请求对象
     * 2）发送抓取数据请求
     * 3）获取抓取数据响应，其中包含了获取的数据
     * 4）由于获取的数据为字节码，还需要转换为字符串，用于业务的操作。
     * 5）将获取的多条数据封装为集合。
     *
     * @param kafkaHost
     * @param kafkaPort
     * @param topic
     * @param partition
     * @param offset
     * @return
     * @throws Exception
     */
    @SuppressWarnings("all")
    public static List<String> getDatas(String kafkaHost, int kafkaPort, String topic, int partition, int offset ) throws Exception {

        // 使用低级API读取数据
        // 获取kafka消费者
        SimpleConsumer consumer = new SimpleConsumer(kafkaHost, kafkaPort, 100000, 10 * 1024, "leader");

        // 获取kafka的元数据信息

        // 主题元数据请求对象
        TopicMetadataRequest request = new TopicMetadataRequest(Arrays.asList(topic));

        // 获取主题元数据响应
        TopicMetadataResponse response = consumer.send(request);

        // 主副本
        BrokerEndPoint leader = null;

        // 响应对象中包含了主题的元数据信息
        leaderLabel:
        for (TopicMetadata topicMetadata : response.topicsMetadata()) {
            // 主题的名称
            String stopic = topicMetadata.topic();
            // 判断是否为我们需要的主题数据
            if ( topic.equals(stopic) ) {
                // 循环主题的所有分区信息
                for (PartitionMetadata partitionMetadata : topicMetadata.partitionsMetadata()) {
                    // 获取分区号
                    int partNum = partitionMetadata.partitionId();
                    if ( partNum == partition ) {
                        // 获取主副本
                        leader = partitionMetadata.leader();
                        break leaderLabel;
                    }
                }
            }
        }

        if ( leader == null ) {
            System.out.println("指定信息无法获取");
            return null;
        } else {

            List<String> dataList = new ArrayList<String>();

            // 创建Leader消费者，连接leader获取数据
            SimpleConsumer dataConsumer = new SimpleConsumer(leader.host(), leader.port(), 100000, 10 * 1024, "accessLeader");

            // 构建抓取请求
            FetchRequest req = new FetchRequestBuilder().addFetch(
                    topic, partition, offset, 10 * 1024
            ).build();

            // 获取抓取响应
            FetchResponse resp = dataConsumer.fetch(req);

            ByteBufferMessageSet ms = resp.messageSet(topic, partition);

            for (MessageAndOffset m : ms) {
                ByteBuffer buff = m.message().payload();
                byte[] bs = new byte[buff.limit()];
                buff.get(bs);
                String val = new String(bs, "UTF-8");
                dataList.add(val);
            }

            return dataList;
        }
    }
}
