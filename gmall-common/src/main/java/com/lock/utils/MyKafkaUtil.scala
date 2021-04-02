package com.lock.utils

import java.util.Properties

import kafka.serializer.StringDecoder
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.{InputDStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils

object MyKafkaUtil {

  // spark 从 kafka 读取数据的两种方式  https://blog.csdn.net/weixin_43283487/article/details/106688763
  // kafka 的 offset 管理方式  https://blog.csdn.net/shichen2010/article/details/104456208

  // Direct 方式
  def getKafkaStream(ssc: StreamingContext, topics: Set[String]): InputDStream[(String, String)] = {

    val properties: Properties = PropertiesUtil.load("config.properties")

    val kafkaPara: Map[String, String] = Map[String, String](
      "bootstrap.servers" -> properties.getProperty("kafka.broker.list"),
      "group.id" -> "bigdata0408"
    )

    //基于Direct方式消费Kafka数据
    val kafkaDStream: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](

      ssc,
      kafkaPara,
      topics)

    //返回
    kafkaDStream
  }

  //  Receiver 方式

  def getKafkaReceiverStream(ssc: StreamingContext, topics: Set[String]): InputDStream[(String, String)] = {

    // 2、topic_name与numThreads的映射
    // topic有几个partition,就写几个numThreads。
    // 每个partition对应一个单独线程从kafka取数据到Spark Streaming
    val topics = Map("topic_black" -> 3)
    val properties: Properties = PropertiesUtil.load("config.properties")

    val kafkaPara: Map[String, String] = Map[String, String](
      "bootstrap.servers" -> properties.getProperty("kafka.broker.list"),
      "group.id" -> "bigdata0408"
    )
    val kafkaReceiverDStream: ReceiverInputDStream[(String, String)] = KafkaUtils.
      createStream[String, String, StringDecoder, StringDecoder](ssc, kafkaPara, topics, StorageLevel.MEMORY_ONLY_SER)
    kafkaReceiverDStream
  }


}
