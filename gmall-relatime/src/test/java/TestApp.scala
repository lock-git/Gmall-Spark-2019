import com.atguigu.utils.MyKafkaUtil
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

object TestApp {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("aaa").setMaster("local[*]")
    val ssc = new StreamingContext(conf, Seconds(5))

    val unit: InputDStream[(String, String)] = MyKafkaUtil.getKafkaStream(ssc, Set("aaa"))

    unit.print()

    ssc.start()
    ssc.awaitTermination()
  }

}
