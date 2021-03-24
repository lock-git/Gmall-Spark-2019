package com.lock.kafka.stream;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.processor.TopologyBuilder;

import java.io.UnsupportedEncodingException;
import java.util.Properties;

public class TranferStream implements Processor<byte[], byte[]> {

    private ProcessorContext context;

    public void init(ProcessorContext context) {
       this.context = context;
    }

    public void process(byte[] key, byte[] value) {


        try {
            // 获取数据value
            String val = new String(value, "UTF-8");
            // 将其中的特殊内容进行转换
            //val = val.replaceAll("atguigu>>>", "");
            val = val.replaceAll("atguigu>>>", "");
            // 通过上下文对象（环境对象）发送到目的地
            context.forward(key, val.getBytes("UTF-8"));
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
    }

    public void punctuate(long timestamp) {

    }

    public void close() {

    }

    public static void main(String[] args) {

        String fromTopic = "first";
        String toTopic = "second";

        TopologyBuilder builder = new TopologyBuilder();
        builder.addSource("SOURCE", fromTopic);
        builder.addProcessor("PROCESSER", new ProcessorSupplier(){
            public Processor get() {
                return new TranferStream();
            }
        }, "SOURCE");
        builder.addSink("SINK", toTopic, "PROCESSER");

        Properties prop = new Properties();
        prop.put(StreamsConfig.APPLICATION_ID_CONFIG, "logFilter");
        prop.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "linux1:9092");


        KafkaStreams streams = new KafkaStreams(builder, prop);
        streams.start();

    }
}
