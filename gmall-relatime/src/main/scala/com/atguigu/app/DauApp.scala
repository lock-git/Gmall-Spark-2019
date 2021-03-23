package com.atguigu.app

import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.JSON
import com.atguigu.bean.StartUpLog
import com.atguigu.constants.GmallConstants
import com.atguigu.handler.DAUHandler
import com.atguigu.utils.MyKafkaUtil
import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object DauApp {

  def main(args: Array[String]): Unit = {

    //1.创建SparkConf
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("DauApp")

    //2.创建StreamingContext
    val ssc = new StreamingContext(conf, Seconds(5))

    val sdf = new SimpleDateFormat("yyyy-MM-dd HH")

    //3.读取Kafka 启动日志主题的数据
    val kafkaStream: InputDStream[(String, String)] = MyKafkaUtil.getKafkaStream(ssc, Set(GmallConstants.GMALL_STARTUP))

    //4.转换为样例类对象
    val startUpLogDStream: DStream[StartUpLog] = kafkaStream.map { case (_, value) =>

      val startLog: StartUpLog = JSON.parseObject(value, classOf[StartUpLog])

      //取出时间戳
      val ts: Long = startLog.ts
      val dateHour: String = sdf.format(new Date(ts))

      //将日期和小时切分开
      val dateHourArr: Array[String] = dateHour.split(" ")

      //给startLog对象赋值，日期和小时
      startLog.logDate = dateHourArr(0)
      startLog.logHour = dateHourArr(1)

      //返回
      startLog
    }

    //5.Redis去重（不同批次）
    val filterStartUpLogDStream: DStream[StartUpLog] = DAUHandler.filterDataByRedis(ssc, startUpLogDStream)

    //6.去重（相同批次）
    val distinctStartUpLogDStream: DStream[StartUpLog] = filterStartUpLogDStream.map(log => (log.mid, log))
      .groupByKey()
      .flatMap { case (mid, logIter) =>
        logIter.toList.take(1)
      }

    distinctStartUpLogDStream.cache()

    distinctStartUpLogDStream.foreachRDD(rdd => {
      println(s"第二次去重后：${rdd.count()}")
      println("*****************************")
    })

    //7.将去重后写入Redis
    DAUHandler.saveUserToRedis(distinctStartUpLogDStream)

    //8.写入HBase
    distinctStartUpLogDStream.foreachRDD(rdd => {
      import org.apache.phoenix.spark._
      rdd.saveToPhoenix("GMALL190408_DAU", Seq("MID", "UID", "APPID", "AREA", "OS", "CH", "TYPE", "VS", "LOGDATE", "LOGHOUR", "TS"), new Configuration, Some("hadoop102,hadoop103,hadoop104:2181"))
    })

    //测试，打印
    //kafkaStream.map(_._2).print()

    //启动
    ssc.start()
    ssc.awaitTermination()
  }
}
