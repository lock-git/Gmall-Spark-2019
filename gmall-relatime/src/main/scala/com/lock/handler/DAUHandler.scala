package com.lock.handler

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.lock.bean.StartUpLog
import com.lock.utils.RedisUtil
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import redis.clients.jedis.Jedis

object  DAUHandler {

  //时间转换对象
  private val sdf = new SimpleDateFormat("yyyy-MM-dd")

  /**
    * 不同批次去重
    *
    * @param startUpLogDStream 原始数据
    */
  def filterDataByRedis(ssc: StreamingContext, startUpLogDStream: DStream[StartUpLog]): DStream[StartUpLog] = {

    startUpLogDStream.transform(rdd => {

      println(s"第一次去重前：${rdd.count()}")

      //获取Redis中的mids
      val jedis: Jedis = RedisUtil.getJedisClient
      val date: String = sdf.format(new Date(System.currentTimeMillis()))
      val mids: util.Set[String] = jedis.smembers(s"dau:$date")
      val midsBC: Broadcast[util.Set[String]] = ssc.sparkContext.broadcast(mids)
      jedis.close()

      //去重
      val filterStartUpLogRDD: RDD[StartUpLog] = rdd.filter(log => {

        val midsBCValue: util.Set[String] = midsBC.value

        !midsBCValue.contains(log.mid)
      })

      println(s"第一次去重后：${filterStartUpLogRDD.count()}")

      //返回
      filterStartUpLogRDD
    })

  }


  /**
    * 将数据存入Redis
    *
    * @param startUpLogDStream 过滤后的数据集
    */
  def saveUserToRedis(startUpLogDStream: DStream[StartUpLog]): Unit = {

    startUpLogDStream.foreachRDD(log => {

      log.foreachPartition(items => {

        //获取Redis连接
        val jedis: Jedis = RedisUtil.getJedisClient

        //遍历写出
        items.foreach(startLog => {

          //RedisKey
          val redisKey = s"dau:${startLog.logDate}"
          jedis.sadd(redisKey, startLog.mid)
        })

        //关闭Redis连接
        jedis.close()
      })

    })


  }

}
