package com.atguigu.HBase.BulkLoad

import com.atguigu.utils.SparkUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, RegionLocator, Table}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{HFileOutputFormat2, LoadIncrementalHFiles}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, HConstants, KeyValue, TableName}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * author  Lock.xia
  * Date 2021-03-30
  *
  *  BUG 解答：
  *  https://www.dazhuanlan.com/2020/03/28/5e7e522157547/
  */
object BulkLoadSparkHBase {

  def main(args: Array[String]): Unit = {

    val sparkSession: SparkSession = SparkUtils.getSparkSession("BulkLoadSparkHBase")
    sparkSession.sparkContext.setLogLevel("ERROR")

    val sc: SparkContext = sparkSession.sparkContext

    val data = List(//测试数据
      ("abc", ("ext", "type", "login")),
      ("ccc", ("ext", "type", "logout"))
    )
    val dataRdd: RDD[(String, (String, String, String))] = sc.parallelize(data)

    val outputRDD: RDD[(ImmutableBytesWritable, KeyValue)] = dataRdd.map {
      x: (String, (String, String, String)) => {
        val rowKey: Array[Byte] = Bytes.toBytes(x._1)
        val immutableRowKey = new ImmutableBytesWritable(rowKey)

        val colFam: String = x._2._1
        val colName: String = x._2._2
        val colValue: String = x._2._3

        val kv: KeyValue = new KeyValue(
          rowKey,
          Bytes.toBytes(colFam),
          Bytes.toBytes(colName),
          Bytes.toBytes(colValue.toString)
        )
        (immutableRowKey, kv)
      }
    }


    val hConf: Configuration = HBaseConfiguration.create()
    val hTableName = "test_log"
    hConf.set("hbase.mapreduce.hfileoutputformat.table.name", hTableName)
    hConf.set(LoadIncrementalHFiles.MAX_FILES_PER_REGION_PER_FAMILY, "1024")
    hConf.set(HConstants.HREGION_MAX_FILESIZE, "10737418240")

    // conf.setInt("hbase.mapreduce.bulkload.max.hfiles.perRegion.perFamily", 1024);
    // --conf spark.yarn.tokens.hbase.enabled=true

    val tableName: TableName = TableName.valueOf(hTableName)
    val conn: Connection = ConnectionFactory.createConnection(hConf)
    val table: Table = conn.getTable(tableName)
    val regionLocator: RegionLocator = conn.getRegionLocator(tableName)

    val hFileOutput = "/tmp/h_file"

    outputRDD.saveAsNewAPIHadoopFile(hFileOutput,
      classOf[ImmutableBytesWritable],
      classOf[KeyValue],
      classOf[HFileOutputFormat2],
      hConf
    )

    val bulkLoader = new LoadIncrementalHFiles(hConf)
    bulkLoader.doBulkLoad(new Path(hFileOutput), conn.getAdmin, table, regionLocator)
  }



  

}
