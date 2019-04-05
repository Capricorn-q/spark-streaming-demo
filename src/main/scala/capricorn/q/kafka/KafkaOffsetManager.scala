package capricorn.q.kafka

import org.I0Itec.zkclient.ZkClient
import org.apache.kafka.common.TopicPartition
import org.apache.log4j.Logger

/**
  *
  * @Description : KafkaOffsetManager
  * @Author : Capricorn.QBB
  * @Date : 2019-04-05
  * @Version : 1.0
  */
object KafkaOffsetManager {

  @transient lazy val log: Logger = org.apache.log4j.LogManager.getLogger(this.getClass)

  /**
    *
    * @param zk
    * @param group
    * @param topic
    * @return
    */
  def readOffsets(zk: ZkClient, group: String, topic: String): Option[Map[TopicPartition, Long]] = {

    // TODO check change of the kafka partitions
    //    val zku = ZkUtils.apply("127.0.0.1:2181", 30000, 30000, JaasUtils.isZkSecurityEnabled)
    //    val tm = AdminUtils.fetchTopicMetadataFromZk(topic, zku)
    //    val pms: util.List[MetadataResponse.PartitionMetadata] = result.partitionMetadata()

    val path = KafkaOffsetManager.getZkOffsetPath(group, topic)
    if (zk.exists(path)) {
      log.debug(s"read offsets, zookeeper ptah : $path")
      val value = zk.readData(path).toString
      val fromOffsets = value.split(",")
        .map(s => s.split(":"))
        .map { case Array(partitionStr, offsetStr) => new TopicPartition(topic, partitionStr.toInt) -> offsetStr.toLong }
        .toMap
      Some(fromOffsets)
    } else
      None
  }

  /**
    *
    * @param zk
    * @param group
    * @param topic
    * @param value
    */
  def saveOffsets(zk: ZkClient, group: String, topic: String, value: String): Unit = {
    val path = getZkOffsetPath(group, topic)
    log.debug(s"save offsets, zookeeper path : $path , values : $value")
    if (zk.exists(path))
      zk.writeData(path, value)
    else {
      zk.createPersistent(path, true)
      zk.writeData(path, value)
    }
  }

  /**
    *
    * @param group
    * @param topic
    * @return
    */
  def getZkOffsetPath(group: String, topic: String): String =
    if (group == null) s"/kafkaOffsets/$topic"
    else s"/kafkaOffsets/$group/$topic"
}
