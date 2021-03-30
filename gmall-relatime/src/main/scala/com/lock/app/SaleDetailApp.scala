package com.lock.app

import java.util

import com.alibaba.fastjson.JSON
import com.lock.bean.{OrderDetail, OrderInfo, SaleDetail, UserInfo}
import com.lock.constants.GmallConstants
import com.lock.utils.{MyEsUtil, MyKafkaUtil, RedisUtil}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.json4s.DefaultFormats
import redis.clients.jedis.Jedis
import org.json4s.native.Serialization

import scala.collection.mutable.ListBuffer

object SaleDetailApp {

  def main(args: Array[String]): Unit = {

    //1.创建SparkConf
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("OrderApp")

    //2.创建StreamingContext
    val ssc = new StreamingContext(conf, Seconds(5))

    //3.读取kafka三个主题的数据
    val inputOrderInfoDStream: InputDStream[(String, String)] = MyKafkaUtil.getKafkaStream(ssc, Set(GmallConstants.GMALL_ORDER_INFO))
    val inputOrderDetailDStream: InputDStream[(String, String)] = MyKafkaUtil.getKafkaStream(ssc, Set(GmallConstants.GMALL_ORDER_DETAIL))
    val inputUserInfoDStream: InputDStream[(String, String)] = MyKafkaUtil.getKafkaStream(ssc, Set(GmallConstants.GMALL_USER_INFO))

    //4.订单信息转换为样例类对象=>(id,orderInfo)
    val idToOrderInfoDStream: DStream[(String, OrderInfo)] = inputOrderInfoDStream.map { case (_, value) =>

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

      (orderInfo.id, orderInfo)
    }

    //5.订单详情转换为样例类对象=>(order_id,orderDetail)
    val orderIdToOrderDetailDStream: DStream[(String, OrderDetail)] = inputOrderDetailDStream.map { case (key, value) =>
      val orderDetail: OrderDetail = JSON.parseObject(value, classOf[OrderDetail])
      (orderDetail.order_id, orderDetail)
    }

    //6.订单表Join订单详情表
    val orderJoinDetailDStream: DStream[(String, (Option[OrderInfo], Option[OrderDetail]))] = idToOrderInfoDStream.fullOuterJoin(orderIdToOrderDetailDStream)

    //7.将join结果数据整理为SalaDetail
    val saleDetailDStream: DStream[SaleDetail] = orderJoinDetailDStream.mapPartitions((iter: Iterator[(String, (Option[OrderInfo], Option[OrderDetail]))]) => {

      //获取Redis连接
      val jedis: Jedis = RedisUtil.getJedisClient
      val saleDetailList = new ListBuffer[SaleDetail]()
      implicit val formats: DefaultFormats.type = org.json4s.DefaultFormats
      iter.foreach { case (orderId, (orderInfoOption, orderDetailOption)) =>

        //判断orderInfoOption是否为空
        if (orderInfoOption.isDefined) {
          //主表不为空
          val orderInfo: OrderInfo = orderInfoOption.get

          //a.判读orderDetailOption是否为空
          if (orderDetailOption.isDefined) {
            val orderDetail: OrderDetail = orderDetailOption.get
            val saleDetail = new SaleDetail(orderInfo, orderDetail)
            //将关联上的数据添加至集合
            saleDetailList += saleDetail
          }

          //b.主表不为空，将其写入缓存（redis） type:String,key:"order_info:$order_id",value:orderInfoJson
          val orderInfoKey = s"order_info:$orderId"
          //无法转换Scala对象
          //val orderInfoJson: String = JSON.toJSONString(orderInfo)
          val orderInfoJson: String = Serialization.write(orderInfo)
          jedis.setex(orderInfoKey, 300, orderInfoJson)

          //c.查询Redis中orderDetail数据
          val orderDetailKey = s"order_detail:$orderId"
          val orderDetailSet: util.Set[String] = jedis.smembers(orderDetailKey)
          if (!orderDetailSet.isEmpty) {
            import collection.JavaConversions._
            for (orderDetailJson: String <- orderDetailSet) {
              //转换为OrderDetail对象
              val orderDetail: OrderDetail = JSON.parseObject(orderDetailJson, classOf[OrderDetail])
              val saleDetail = new SaleDetail(orderInfo, orderDetail)
              saleDetailList += saleDetail
            }
          }
          //主表为空，从表不为空
        } else if (orderDetailOption.isDefined) {
          val orderDetail: OrderDetail = orderDetailOption.get

          //a.查询order_info的缓存
          val orderInfoKey = s"order_info:$orderId"
          val orderInfoJson: String = jedis.get(orderInfoKey)
          if (!orderInfoJson.isEmpty && orderInfoJson != null) {
            //转换为OrderInfo对象
            val orderInfo: OrderInfo = JSON.parseObject(orderInfoJson, classOf[OrderInfo])
            val saleDetail = new SaleDetail(orderInfo, orderDetail)
            saleDetailList += saleDetail
          }

          //b.将orderDetail转换为JSON写入Redis
          val orderDetailJson: String = Serialization.write(orderDetail)
          val orderDetailKey = s"order_detail:$orderId"
          jedis.sadd(orderDetailKey, orderDetailJson)
          jedis.expire(orderDetailKey, 300)
        }
      }

      //关闭Redis连接
      jedis.close()
      saleDetailList.toIterator
    })

    //将用户信息写入Redis
    inputUserInfoDStream.foreachRDD((rdd: RDD[(String, String)]) => {

      rdd.foreachPartition((iter: Iterator[(String, String)]) => {

        //获取Redis连接
        val jedis: Jedis = RedisUtil.getJedisClient

        //遍历数据写入Redis
        iter.foreach { case (_, userInfoJson) =>
          val userInfo: UserInfo = JSON.parseObject(userInfoJson, classOf[UserInfo])
          val userInfoKey = s"userInfo:${userInfo.id}"
          jedis.set(userInfoKey, userInfoJson)
        }
        //关闭Redis连接
        jedis.close()
      })
    })

    //对saleDetailDStream查询Redis中的用户信息进行Join
    val fullSaleDetailDStream: DStream[SaleDetail] = saleDetailDStream.mapPartitions((iter: Iterator[SaleDetail]) => {

      //获取Redis连接
      val jedis: Jedis = RedisUtil.getJedisClient

      //创建集合用于存放合并后的saleDetail
      val saleDetailList = new ListBuffer[SaleDetail]

      iter.foreach((saleDetail: SaleDetail) => {
        val userInfoKey = s"userInfo:${saleDetail.user_id}"
        val userInfoJson: String = jedis.get(userInfoKey)
        if (userInfoJson != null) {
          //将userInfoJson转换为样例类对象
          val userInfo: UserInfo = JSON.parseObject(userInfoJson, classOf[UserInfo])
          saleDetail.mergeUserInfo(userInfo)
          saleDetailList += saleDetail
        }
      })

      //关闭Redis连接
      jedis.close()
      saleDetailList.toIterator
    })

    fullSaleDetailDStream.cache()
    //写入ES
    fullSaleDetailDStream.foreachRDD(rdd => {
      rdd.map(saleDetail => (saleDetail.order_detail_id, saleDetail)).foreachPartition(iter => {
        MyEsUtil.insertEsByBulk(GmallConstants.ES_GMALL_SALE_DETAIL, iter.toList)
      })
    })

    // 检验
    fullSaleDetailDStream.print()

    //启动
    ssc.start()
    ssc.awaitTermination()

  }

}
