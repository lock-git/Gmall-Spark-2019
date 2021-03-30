package com.lock.HBase.BulkLoad

import java.util

import com.lock.utils.SparkUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql
import org.apache.spark.sql.DataFrame

import scala.util.Try

/**
  * author  Lock.xia
  * Date 2021-03-30
  */
object BulkLoadV3 {
  def main(args: Array[String]): Unit = {

    val rowKeyField = "id"
    val df: DataFrame = SparkUtils.getSparkSession("BulkLoadV3").read.format("json").load("/stats.json")
    val fields: Array[String] = df.columns.filterNot((_: String) == "id")

    df.rdd.foreachPartition { partition: Iterator[sql.Row] =>
      val hbaseConf: Configuration = HBaseConfiguration.create()
      hbaseConf.set("hbase.zookeeper.quorum", "linux-1:2181,linux-2:2181,linux-3:2181")
      hbaseConf.set(TableOutputFormat.OUTPUT_TABLE, "batch_put")

      val conn: Connection = ConnectionFactory.createConnection(hbaseConf)
      val table: Table = conn.getTable(TableName.valueOf("batch_put"))

      val puts = new util.ArrayList[Put]()
      partition.foreach { row: sql.Row =>
        val rowKey: Array[Byte] = Bytes.toBytes(row.getAs(rowKeyField).toString)
        val put = new Put(rowKey)
        val family: Array[Byte] = Bytes.toBytes("hfile-fy")

        fields.foreach { field: String =>
          put.addColumn(family, Bytes.toBytes(field), Bytes.toBytes(row.getAs(field).toString))
        }

        puts.add(put)
      }
      Try(table.put(puts)).getOrElse(table.close())

      table.close()
      conn.close()
    }

  }

}
