package com.wajam.spnl

import com.wajam.nrv.zookeeper.ZookeeperClient
import com.wajam.nrv.zookeeper.ZookeeperClient._
import com.wajam.nrv.protocol.codec.JavaSerializeCodec
import com.wajam.nrv.Logging

/**
 * Task persistence that saves job states into zookeeper
 */
class ZookeeperTaskPersistence(zkClient: ZookeeperClient) extends TaskPersistence with Logging {
  val serializer = new JavaSerializeCodec

  def saveTask(task: Task) {
    if (task.lifetime == PERSISTENT_GLOBAL) {
      val path = "/spnl/global/" + task.name
      val data = task.context.toJson

      zkClient.ensureAllExists(path, data)
      zkClient.set(path, data)
    }
  }

  def loadTask(task: Task) {
    //TODO make Manuel and APP happy by fixing paths.
    val path = "/spnl/global/" + task.name
    if (task.lifetime == PERSISTENT_GLOBAL && zkClient.exists(path)) {
      val data = zkClient.getString(path)
      try {
        task.context = TaskContext.fromJson(data)
      } catch {
        case e: Exception => warn("Couldn't unserialize task {} from zookeeper", task.name, e)
      }
    }
  }
}
