package com.wajam.spnl

import com.wajam.nrv.zookeeper.ZookeeperClient
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
      val data = serializeTask(task)

      zkClient.ensureAllExists(path, data)
      zkClient.set(path, data)
    }
  }

  def loadTask(task: Task) {
    if (task.lifetime == PERSISTENT_GLOBAL) {
      val data = zkClient.get("/spnl/global/" + task.name)
      unserializeTask(task, data)
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
