package com.atguigu.utils

import java.util
import java.util.Objects

import io.searchbox.client.{JestClient, JestClientFactory}
import io.searchbox.client.config.HttpClientConfig
import io.searchbox.core.{Bulk, BulkResult, Index}

object MyEsUtil {

  private val ES_HOST = "http://hadoop102"
  private val ES_HTTP_PORT = 9200
  private var factory: JestClientFactory = _

  /**
    * 获取客户端
    *
    * @return jestclient
    */
  def getClient: JestClient = {
    if (factory == null) build()
    factory.getObject
  }

  /**
    * 关闭客户端
    */
  def close(client: JestClient): Unit = {
    if (!Objects.isNull(client)) try
      client.shutdownClient()
    catch {
      case e: Exception =>
        e.printStackTrace()
    }
  }

  /**
    * 建立连接
    */
  private def build(): Unit = {
    factory = new JestClientFactory
    factory.setHttpClientConfig(new HttpClientConfig.Builder(ES_HOST + ":" + ES_HTTP_PORT)
      .multiThreaded(true)
      .maxTotalConnection(200) //连接总数
      .connTimeout(10000)
      .readTimeout(10000)
      .build)
  }

  //批量插入数据
  def insertEsByBulk(indexName: String, list: List[(String, Any)]): Unit = {

    if (list.nonEmpty) {
      val jest: JestClient = getClient

      val bulkBuilder: Bulk.Builder = new Bulk.Builder().defaultIndex(indexName).defaultType("_doc")

      //遍历，封装成index
      for ((key, value) <- list) {
        //给每一条数据构建Index
        val index: Index = new Index.Builder(value).id(key).build()
        //将Index添加至批处理中
        bulkBuilder.addAction(index)
      }

      //批量插入数据
      val items: util.List[BulkResult#BulkResultItem] = jest.execute(bulkBuilder.build()).getItems
      println(s"成功插入${items.size()}条数据")

      //关闭连接
      jest.close()
      close(jest)
    }
  }


  def main(args: Array[String]): Unit = {

    //获取客户端
    val jest: JestClient = getClient

    val index: Index = new Index.Builder(stu("zhaoliu", 18))
      .index("test")
      .`type`("_doc")
      .id("5")
      .build()

    jest.execute(index)

    close(jest)
  }

  case class stu(name: String, age: Long)


}
