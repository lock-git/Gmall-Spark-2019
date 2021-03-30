package com.atguigu.HBase.BulkLoad

import com.atguigu.utils.SparkUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.sql
import org.apache.spark.sql.{DataFrame, SparkSession}


/**
  * author  Lock.xia
  * Date 2021-03-30
  */
object BulkLoadV4 {
  def main(args: Array[String]): Unit = {

    val sparkSession: SparkSession = SparkUtils.getSparkSession("BulkLoadV4")
    val hbaseConf: Configuration = sparkSession.sessionState.newHadoopConf()
    hbaseConf.set("hbase.zookeeper.quorum", "linux-1:2181,linux-2:2181,linux-3:2181")

    hbaseConf.set(TableOutputFormat.OUTPUT_TABLE, "direct")
    val job: Job = Job.getInstance(hbaseConf)
    job.setMapOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setMapOutputValueClass(classOf[Result])
    job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])

    val rowKeyField = "id"

    val df: DataFrame = sparkSession.read.format("json").load("/stats.json")

    val fields: Array[String] = df.columns.filterNot((_: String) == "id")

    df.rdd.map { row: sql.Row =>
      val put = new Put(Bytes.toBytes(row.getAs(rowKeyField).toString))

      val family: Array[Byte] = Bytes.toBytes("hfile-fy")

      fields.foreach { field: String =>
        put.addColumn(family, Bytes.toBytes(field), Bytes.toBytes(row.getAs(field).toString))
      }

      (new ImmutableBytesWritable(), put)
    }.saveAsNewAPIHadoopDataset(job.getConfiguration)

  }
}
