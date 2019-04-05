package capricorn.q.hbase.hclient

import java.util.concurrent.Executors

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory}

/**
  *
  * @Description : HBaseUtil
  * @Author : Capricorn.QBB
  * @Date : 2019-04-05
  * @Version : 1.0
  */
object HBaseUtil {

  private val pool = Executors.newFixedThreadPool(5)

  def getBatchCon(): Connection = {
    ConnectionFactory.createConnection(getHBConf(), pool)
  }

  def getCon(): Connection = {
    ConnectionFactory.createConnection(getHBConf())
  }

  def getHBConf(): Configuration = {
    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum", "127.0.0.1:2181")
    conf
  }

  def main(args: Array[String]): Unit = {
    HBaseUtil.getBatchCon()
  }

}
