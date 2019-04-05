package capricorn.q.kafka

import org.I0Itec.zkclient.serialize.ZkSerializer
import org.apache.commons.io.Charsets

/**
  *
  * @Description : MyZkSerializer
  * @Author : Capricorn.QBB
  * @Date : 2019-04-05
  * @Version : 1.0
  */
object MyZkSerializer extends ZkSerializer {
  override def serialize(o: Any): Array[Byte] = String.valueOf(o).getBytes(Charsets.UTF_8)

  override def deserialize(bytes: Array[Byte]): AnyRef = new String(bytes, Charsets.UTF_8)
}
