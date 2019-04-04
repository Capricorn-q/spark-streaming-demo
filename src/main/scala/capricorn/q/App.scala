package capricorn.q

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Hello world!
  *
  */
object App {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("WordCount").setMaster("local[3]")
    val ssc = new StreamingContext(conf, Seconds(15))
    ssc.sparkContext.setLogLevel("warn")
    val lines = ssc.socketTextStream("localhost", 9999)
    lines.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _).print()
    ssc.start()
    ssc.awaitTermination()
  }
}
