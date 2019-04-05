package capricorn.q.hbase.phoenix

import java.sql.{Connection, DriverManager}
import java.util

/**
  *
  * @Description : PhoenixUtil
  * @Author : Capricorn.QBB
  * @Date : 2019-04-05
  * @Version : 1.0
  */
object PhoenixUtil {

  private var connectionQueue: util.LinkedList[Connection] = _
  private val QUEUE_LEN = 5

  def getConnection(zkUrl: String): Connection = synchronized {
    try
        if (connectionQueue == null) {
          Class.forName("org.apache.phoenix.jdbc.PhoenixDriver")
          connectionQueue = new util.LinkedList[Connection]
          for (i <- 0 until QUEUE_LEN)
            connectionQueue.push(DriverManager.getConnection("jdbc:phoenix:" + zkUrl))
        }
    catch {
      case e: Exception =>
        e.printStackTrace()
    }
    connectionQueue.poll
  }

  def returnConnection(conn: Connection): Unit = {
    connectionQueue.push(conn)
  }


}
