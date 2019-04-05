package capricorn.q.hbase.hclient

import java.util

import capricorn.q.domain.User
import com.alibaba.fastjson.JSON
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  *
  * @Description : HBaseApp, 使用HBase客户端api
  * @Author : Capricorn.QBB
  * @Date : 2019-04-05
  * @Version : 1.0
  */
object HBaseApp {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(" hbase demo ").setMaster("local[3]")
    val ssc = new StreamingContext(conf, Seconds(5))
    ssc.sparkContext.setLogLevel("warn")
    val lines = ssc.socketTextStream("localhost", 12345)

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
      r => r.foreachPartition {
        p =>
          val connection = HBaseUtil.getBatchCon()
          val table = connection.getTable(TableName.valueOf("test:user"))
          val puts = new util.ArrayList[Put]
          p.foreach(
            r => {
              val put: Put = new Put(Bytes.toBytes(r.getId))
              put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes(r.getName))
              put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("age"), Bytes.toBytes(r.getAge))
              put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("address"), Bytes.toBytes(r.getAddress))
              puts.add(put)
              if (puts.size % 10000 == 0) {
                table.put(puts)
                puts.clear()
              }
            }
          )
          if (puts.size() > 0)
            table.put(puts)
          table.close()
          connection.close()
      }
    )

    ssc.start()
    ssc.awaitTermination()
  }
}
