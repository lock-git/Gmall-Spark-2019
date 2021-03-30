package com.atguigu.HBase.BulkLoad

import com.atguigu.utils.SparkUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{HFileOutputFormat2, LoadIncrementalHFiles}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}


/**
  * author  Lock.xia
  * Date 2021-03-30
  */
object BulkLoadV2 {

  def main(args: Array[String]): Unit = {

    val sparkSession: SparkSession = SparkUtils.getSparkSession("BulkLoadV2")

    val rowKeyField = "id"

    val df: DataFrame = sparkSession.read.format("json").load("/people.json")

    val fields: Array[String] = df.columns.filterNot((_: String) == "id").sorted

    val data: RDD[(ImmutableBytesWritable, KeyValue)] = df.rdd.map { row =>
      val rowKey: Array[Byte] = Bytes.toBytes(row.getAs(rowKeyField).toString)

      val kvs: Array[KeyValue] = fields.map { field: String =>
        new KeyValue(rowKey, Bytes.toBytes("hfile-fy"), Bytes.toBytes(field), Bytes.toBytes(row.getAs(field).toString))
      }

      (new ImmutableBytesWritable(rowKey), kvs)
    }.flatMapValues((x: Array[KeyValue]) => x).sortByKey()

    val hbaseConf: Configuration = HBaseConfiguration.create()
    val connection: Connection = ConnectionFactory.createConnection(hbaseConf)

    val tableName: TableName = TableName.valueOf("h_file")

    //没有HBase表则创建
    creteHTable(tableName, connection)

    val table: Table = connection.getTable(tableName)

    try {
      val regionLocator: RegionLocator = connection.getRegionLocator(tableName)

      val job: Job = Job.getInstance(hbaseConf)

      job.setMapOutputKeyClass(classOf[ImmutableBytesWritable])
      job.setMapOutputValueClass(classOf[KeyValue])

      HFileOutputFormat2.configureIncrementalLoad(job, table, regionLocator)

      val savePath = "hdfs://linux-1:9000/hfile_save"
      delHdfsPath(savePath, sparkSession)

      job.getConfiguration.set("mapred.output.dir", savePath)

      data.saveAsNewAPIHadoopDataset(job.getConfiguration)

      val bulkLoader = new LoadIncrementalHFiles(hbaseConf)
      bulkLoader.doBulkLoad(new Path(savePath), connection.getAdmin, table, regionLocator)

    } finally {
      //WARN LoadIncrementalHFiles: Skipping non-directory hdfs://linux-1:9000/hfile_save/_SUCCESS 不影响,直接把文件移到HBASE对应HDFS地址了
      table.close()
      connection.close()
    }

    sparkSession.stop()
  }

  def creteHTable(tableName: TableName, connection: Connection): Unit = {
    val admin: Admin = connection.getAdmin

    if (!admin.tableExists(tableName)) {
      val tableDescriptor = new HTableDescriptor(tableName)
      tableDescriptor.addFamily(new HColumnDescriptor(Bytes.toBytes("hfile-fy")))
      admin.createTable(tableDescriptor)
    }
  }

  def delHdfsPath(path: String, sparkSession: SparkSession) {
    val hdfs: FileSystem = FileSystem.get(sparkSession.sessionState.newHadoopConf())
    val hdfsPath: Path = new Path(path)

    if (hdfs.exists(hdfsPath)) {
      //val filePermission = new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.READ)
      hdfs.delete(hdfsPath, true)
    }
  }

}
