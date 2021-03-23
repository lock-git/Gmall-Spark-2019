package com.atguigu.app

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.alibaba.fastjson.JSON
import com.atguigu.bean.{AlertInfo, EventInfo}
import com.atguigu.constants.GmallConstants
import com.atguigu.utils.{MyEsUtil, MyKafkaUtil}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}

import scala.util.control.Breaks._

object AlertApp {

  def main(args: Array[String]): Unit = {

    //时间转换对象
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH")

    //1.创建SparkConf
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("OrderApp")

    //2.创建StreamingContext
    val ssc = new StreamingContext(conf, Seconds(5))

    //3.读取kafka order主题的数据
    val kafkaDStream: InputDStream[(String, String)] = MyKafkaUtil.getKafkaStream(ssc, Set(GmallConstants.GMALL_EVENT))

    //4.转换数据格式（key,value）=>(mid,EventInfo)
    val midToEventDStream: DStream[(String, EventInfo)] = kafkaDStream.map { case (_, value) =>

      //转换为对象
      val eventInfo: EventInfo = JSON.parseObject(value, classOf[EventInfo])

      //赋值时间
      val ts: Long = eventInfo.ts
      val dateArr: Array[String] = sdf.format(new Date(ts)).split(" ")
      eventInfo.logDate = dateArr(0)
      eventInfo.logHour = dateArr(1)

      (eventInfo.mid, eventInfo)
    }

    //5.开窗并分组
    val midToEventIterDStream: DStream[(String, Iterable[EventInfo])] = midToEventDStream.window(Seconds(60)).groupByKey()

    //6.过滤数据的准备工作(mid,【EventInfo...】)=>(flag,AlterInfo)
    val flagToAlertDStream: DStream[(Boolean, AlertInfo)] = midToEventIterDStream.map { case (mid, eventInfoItre) =>

      //  三次不同账号登录（用户）
      //  领取优惠券（行为）,关联的商品ID
      //  没有浏览商品（行为），放一马
      val uids = new util.HashSet[String]()
      val itemids = new util.HashSet[String]()
      val events = new util.ArrayList[String]()

      //定义一个变量，用于记录是否存在点击商品行为
      var flag: Boolean = true
      breakable {
        eventInfoItre.foreach(eventInfo => {

          events.add(eventInfo.evid) //记录用户的行为

          //判断当前是否为领取优惠券行为日志
          if ("coupon".equals(eventInfo.evid)) {
            uids.add(eventInfo.uid) //记录用户数
            itemids.add(eventInfo.itemid)
          } else if ("clickItem".equals(eventInfo.evid)) {
            flag = false
            break()
          }
        })
      }

      (flag && uids.size() >= 3, AlertInfo(mid, uids, itemids, events, System.currentTimeMillis()))
    }

    //7.过滤数据
    val alertInfoDStream: DStream[AlertInfo] = flagToAlertDStream.filter(_._1).map(_._2)
    alertInfoDStream.cache()

    //打印过滤后的数据
    alertInfoDStream.print()

    //8.将数据写入ES  alertInfo=>(mid_分钟，alertInfo)
    alertInfoDStream.map(alertInfo => {
      val mint: Long = alertInfo.ts / 1000 / 60
      (s"${alertInfo.mid}_$mint", alertInfo)
    }).foreachRDD(rdd => {

      //按照分区进行写入数据
      rdd.foreachPartition(iter => {
        MyEsUtil.insertEsByBulk("gmall0408_coupon_alert", iter.toList)
      })
    })

    //9.启动
    ssc.start()
    ssc.awaitTermination()
  }

}
