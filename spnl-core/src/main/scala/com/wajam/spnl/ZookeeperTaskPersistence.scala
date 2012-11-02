package com.wajam.spnl

import com.wajam.nrv.cluster.zookeeper.ZookeeperClient
import com.wajam.nrv.protocol.codec.JavaSerializeCodec
import com.wajam.nrv.Logging

/**
 * Task persistence that saves job states into zookeeper
 */
class ZookeeperTaskPersistence(zkClient: ZookeeperClient) extends TaskPersistence with Logging {
  val serializer = new JavaSerializeCodec

  zkClient.ensureExists("/spnl", "".getBytes)
  zkClient.ensureExists("/spnl/global", "".getBytes)

  def saveTask(task: Task) {
    if (task.lifetime == PERSISTENT_GLOBAL) {
      val path = "/spnl/global/" + task.name
      val data = serializeTask(task)

      zkClient.ensureExists(path, data)
      zkClient.set(path, data)
    }
  }

  def loadTask(task: Task) {
    if (task.lifetime == PERSISTENT_GLOBAL) {
      val path = "/spnl/global/" + task.name
      if (zkClient.exists(path)) {
        val data = zkClient.get(path)
        unserializeTask(task, data)
      }
    }
  }

  def serializeTask(task: Task): Array[Byte] = serializer.encodeAny(task.context)

  def unserializeTask(task: Task, data: Array[Byte]) {
    try {
      task.context = serializer.decodeAny(data).asInstanceOf[TaskContext]
    } catch {
      case e: Exception => warn("Couldn't unserialize task {} from zookeeper", task.name, e)
    }
  }
}
