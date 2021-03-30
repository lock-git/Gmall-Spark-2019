package com.lock.utils

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SparkUtils {

  def getSparkSession(appName: String): SparkSession = {
    val sparkConf = new SparkConf()
      .setAppName(appName)
      .set("hive.exec.dynamic.partition", "true")
      .set("hive.exec.max.created.files", "100000")
      .set("hive.exec.max.dynamic.partitions", "100000")
      .set("hive.exec.dynamic.partition.mode", "nonstrict")
      .set("hive.exec.max.dynamic.partitions.pernode", "100000")
      .set("dfs.permissions", "false")
      .set("hive.metastore.uris", "thrift://node001.cdh.jdd.com:9083")
      .set("spark.network.timeout", "360")
      .set("spark.sql.shuffle.partitions", "10")
      .set("spark.shuffle.service.enabled", "true")
      .set("spark.dynamicAllocation.enabled", "false") //动态申请executor，默认true
      .set("spark.yarn.executor.memoryOverhead", "6144") // M
      .set("spark.sql.autoBroadcastJoinThreshold", "5000")
      .set("spark.debug.maxToStringFields", "5000")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("mapreduce.job.inputformat.class", "org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat")
      .set("mapreduce.input.fileinputformat.split.maxsize", "268435456")
      .set("spark.streaming.kafka.consumer.poll.ms", "10000")
      .set("spark.kryoserializer.buffer.max", "1024m")
      .set("spark.driver.maxResultSize", "4g")
      .set("spark.sql.crossJoin.enabled", "true")
      .set("spark.streaming.concurrentJobs", "10")


    SparkSession.builder.config(sparkConf).enableHiveSupport.getOrCreate
  }
}
