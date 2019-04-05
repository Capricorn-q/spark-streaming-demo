package capricorn.q.hbase.hadoopapi

import capricorn.q.domain.User
import com.alibaba.fastjson.JSON
import org.apache.hadoop.hbase.client.{Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  *
  * @Description : store using saveAsNewAPIHadoopDataset
  * @Author : Capricorn.QBB
  * @Date : 2019-04-05
  * @Version : 1.0
  */
object NewHadoopApiApp {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(" hbase demo ").setMaster("local[3]")
    val ssc = new StreamingContext(conf, Seconds(5))
    ssc.sparkContext.setLogLevel("warn")
    val lines = ssc.socketTextStream("localhost", 12345)

    val sc = ssc.sparkContext
    sc.hadoopConfiguration
      .set(TableOutputFormat.OUTPUT_TABLE, "test:user")

    val job = Job.getInstance(sc.hadoopConfiguration)
    job.setOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setOutputValueClass(classOf[Result])
    job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])

    /**
      * input :
      * {"id":"1","name":"tom","age":"22","address":"gz"}
      * {"id":"2","name":"alice","age":"11","address":"gz"}
      * {"id":"3","name":"jack","age":"33","address":"gz"}
      */
    val ds = lines.map(
      try {
        JSON.parseObject(_, classOf[User])
      } catch {
        case e: Throwable => println(" parse error .. " + e.getMessage)
          null
      }
    ).filter(_ != null)

    ds.print(3)
    ds.foreachRDD(
      _.map(r => {
        val put: Put = new Put(Bytes.toBytes(r.getId))
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes(r.getName))
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("age"), Bytes.toBytes(r.getAge))
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("address"), Bytes.toBytes(r.getAddress))
        (new ImmutableBytesWritable, put)
      }).saveAsNewAPIHadoopDataset(job.getConfiguration)
    )

    ssc.start()
    ssc.awaitTermination()
  }
}
