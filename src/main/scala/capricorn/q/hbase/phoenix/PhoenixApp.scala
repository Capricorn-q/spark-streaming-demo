package capricorn.q.hbase.phoenix

import java.sql.DriverManager

import capricorn.q.domain.User
import com.alibaba.fastjson.JSON
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  *
  * @Description : Phoenix demo
  * @Author : Capricorn.QBB
  * @Date : 2019-04-04
  * @Version : 1.0
  */
object PhoenixApp {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(" phoenix test ").setMaster("local[3]")
    val ssc = new StreamingContext(conf, Seconds(15))
    ssc.sparkContext.setLogLevel("warn")

    val zkUrl = "127.0.0.1:2181"
    val lines = ssc.socketTextStream("127.0.0.1", 12345)

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
    //    ds.foreachRDD(
    //      r => r.foreachPartition {
    //        p =>
    //          p.foreach(
    //            r => {
    //              val con = PhoenixUtils.getConnection(zkUrl)
    //              //                con.setAutoCommit(false)
    //              val stm = con.createStatement()
    //              stm.execute(s"upsert into user(id, INFO.name, INFO.age, INFO.address) values(${r.getId}, '${r.getName}', ${r.getAge},'${r.getAddress}')")
    //              con.commit()
    //              stm.close()
    //              PhoenixUtils.returnConnection(con)
    //            }
    //          )
    //      })


    ds.foreachRDD(
      r => r.foreachPartition {
        p =>
          Class.forName("org.apache.phoenix.jdbc.PhoenixDriver")
          val conn = DriverManager.getConnection("jdbc:phoenix:" + zkUrl)
          //                con.setAutoCommit(false)
          val stm = conn.createStatement()
          p.foreach(
            r => {
              stm.execute(s"upsert into user(id, INFO.name, INFO.age, INFO.address) values(${r.getId}, '${r.getName}', ${r.getAge},'${r.getAddress}')")
            }
          )
          conn.commit()
          stm.close()
          conn.close()
      }
    )

    ssc.start()
    ssc.awaitTermination()
  }

}
