package com.atguigu.app

import com.alibaba.fastjson.JSON
import com.atguigu.bean.OrderInfo
import com.atguigu.constants.GmallConstants
import com.atguigu.utils.MyKafkaUtil
import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object OrderApp {

  def main(args: Array[String]): Unit = {

    //1.创建SparkConf
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("OrderApp")

    //2.创建StreamingContext
    val ssc = new StreamingContext(conf, Seconds(5))

    //3.读取kafka order主题的数据
    val kafkaDStream: InputDStream[(String, String)] = MyKafkaUtil.getKafkaStream(ssc, Set(GmallConstants.GMALL_ORDER_INFO))

    //4.格式化数据为OrderInfo对象，同时对手机号进行脱敏，添加日期和小时
    val orderInfoDStream: DStream[OrderInfo] = kafkaDStream.map { case (_, value) =>

      //转换为OrderInfo对象
      val orderInfo: OrderInfo = JSON.parseObject(value, classOf[OrderInfo])

      //对手机号进行脱敏
      val telArr: (String, String) = orderInfo.consignee_tel.splitAt(4)
      orderInfo.consignee_tel = s"${telArr._1}*******"

      //添加日期和小时
      val create_time_arr: Array[String] = orderInfo.create_time.split(" ")
      orderInfo.create_date = create_time_arr(0)
      val timeArr: Array[String] = create_time_arr(1).split(":")
      orderInfo.create_hour = timeArr(0)

      orderInfo
    }

    //5.写入HBase
    orderInfoDStream.foreachRDD(rdd => {
      println("***************")
      import org.apache.phoenix.spark._
      val configuration = new Configuration
      rdd.saveToPhoenix("GMALL0408_ORDER_INFO", Seq("ID", "PROVINCE_ID", "CONSIGNEE", "ORDER_COMMENT", "CONSIGNEE_TEL", "ORDER_STATUS", "PAYMENT_WAY", "USER_ID", "IMG_URL", "TOTAL_AMOUNT", "EXPIRE_TIME", "DELIVERY_ADDRESS", "CREATE_TIME", "OPERATE_TIME", "TRACKING_NO", "PARENT_ORDER_ID", "OUT_TRADE_NO", "TRADE_BODY", "CREATE_DATE", "CREATE_HOUR"), configuration, Some("hadoop102,hadoop103,hadoop104:2181"))
    })

    //6.启动
    ssc.start()
    ssc.awaitTermination()
  }

}