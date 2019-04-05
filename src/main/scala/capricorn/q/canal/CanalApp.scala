package capricorn.q.canal


import capricorn.q.domain.User
import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable.ListBuffer

/**
  *
  * @Description : 基于alibaba-canal，默认biglog-kafka-json格式解析示例
  *              kafka-version : 0.11.0.2
  *              canal-version : 1.1.2
  * @Author : Capricorn.QBB
  * @Date : 2019-04-04
  * @Version : 1.0
  */
object CanalApp {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setMaster("local[3]") // using master-url 2 deploy
      .setAppName(" canal test ")
    val sc = new StreamingContext(conf, Seconds(5))
    sc.sparkContext.setLogLevel("warn")

    // kafka
    val group = "test"
    val topics = Set("example") // using canal default topic
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "127.0.0.1:9092",
      "key.deserializer" -> classOf[StringDeserializer], "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> group,
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean))

    val lines = KafkaUtils.createDirectStream(
      sc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)).map(_.value().trim)

    /**
      * @param JSONObject
      * @return (pk,(sortKey,Obj))
      */
    def getArrayObj(j: JSONObject): Seq[(String, (String, User))] = {
      val list = ListBuffer[(String, (String, User))]()
      val arr: JSONArray = j.getJSONArray("data")
      val sortK = j.get("id").toString
      for (i <- 0 until arr.size())
        list.append((arr.getJSONObject(i).get("id").toString, (sortK, arr.getJSONObject(i).toJavaObject(classOf[User]))))
      list.toList
    }

    val table = "user"

    lines.map(
      try {
        JSON.parseObject(_)
      } catch {
        case e: Throwable => println(" parse error .. " + e.getMessage)
          null
      }).filter(_ != null).filter(_.get("table").equals(table))
      .flatMap(getArrayObj) // get the record
      .reduceByKey((x, y) => if (x._1 > y._1) x else y) // simple merge
      .print()

    sc.start()
    sc.awaitTermination()
  }
}
