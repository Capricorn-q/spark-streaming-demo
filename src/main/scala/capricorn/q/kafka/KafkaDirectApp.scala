package capricorn.q.kafka

import org.I0Itec.zkclient.ZkClient
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, KafkaUtils}
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent

/**
  *
  * @Description : kafka direct demo
  *              使用zk管理kafka偏移量
  * @Author : Capricorn.QBB
  * @Date : 2019-04-05
  * @Version : 1.0
  */
object KafkaDirectApp {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setMaster("local[3]") // using master-url 2 deploy
      .setAppName(" kafka test ")

    conf.set("spark.streaming.backpressure.enabled", "true")
    conf.set("spark.streaming.stopGracefullyOnShutdown", "true")
    conf.set("spark.streaming.backpressure.initialRate", "5000")
    conf.set("spark.streaming.stopGracefullyOnShutdown", "true")
    conf.set("spark.streaming.kafka.maxRatePerPartition", "2000")

    val sc = new StreamingContext(conf, Seconds(5)) //rolling time
    sc.sparkContext.setLogLevel("WARN")

    // zookeeper
    val zkUrl = "127.0.0.1:2181"
    val zk = new ZkClient(zkUrl)

    // kafka
    val group = "test"
    val topics = Set("test")
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "127.0.0.1:9092",
      "key.deserializer" -> classOf[StringDeserializer], "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> group,
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val stream = createKafkaStream(sc, kafkaParams, zk, topics)

    val wc = stream.map(_.value().trim).flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)
    wc.print()

    //    store the offsets
    stream.foreachRDD(
      rdd => {
        val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        val offsetsRangesStr = offsetRanges.map(offsetRange => s"${offsetRange.partition}:${offsetRange.untilOffset}").mkString(",")
        KafkaOffsetManager.saveOffsets(zk, group, topics.last, offsetsRangesStr)
      }
    )

    sc.start()
    sc.awaitTermination()
  }

  def createKafkaStream(ssc: StreamingContext,
                        kafkaParams: Map[String, Object],
                        zk: ZkClient,
                        topics: Set[String]
                       ): InputDStream[ConsumerRecord[String, String]] = {

    val m = KafkaOffsetManager.readOffsets(zk, kafkaParams("group.id").toString, topics.last)
    val kafkaStream = m match {
      case Some(fromOffsets) =>
        KafkaUtils.createDirectStream[String, String](
          ssc,
          PreferConsistent,
          Subscribe[String, String](topics, kafkaParams, fromOffsets))
      //       Assign[String, String](fromOffsets.keys.toList, kafkaParams, fromOffsets) // don`t work
      case None =>
        KafkaUtils.createDirectStream[String, String](
          ssc,
          PreferConsistent,
          Subscribe[String, String](topics, kafkaParams))
    }
    kafkaStream
  }
}
