package com.wajam.spnl

import com.wajam.nrv.zookeeper.ZookeeperClient
import com.wajam.nrv.zookeeper.ZookeeperClient._
import com.wajam.nrv.Logging
import com.wajam.nrv.service.{ServiceMember, Service}
import com.wajam.nrv.zookeeper.service.ZookeeperService

/**
 * Task persistence that saves job states into zookeeper
 */
class ZookeeperTaskPersistence(zkClient: ZookeeperClient, service: Service, member: ServiceMember)
  extends TaskPersistence with Logging {

  def saveTask(task: Task) {
    val path = ZookeeperTaskPersistence.serviceMemberPath(service, member, task.name)
    val data = task.context.toJson

    zkClient.ensureAllExists(path, data)
    zkClient.set(path, data)
  }

  def loadTask(task: Task) {
    val path = ZookeeperTaskPersistence.serviceMemberPath(service, member, task.name)
    if (zkClient.exists(path)) {
      val data = zkClient.getString(path)
      try {
        task.context.updateFromJson(data)
      } catch {
        case e: Exception => warn("Couldn't unserialize task {} from zookeeper", task.name, e)
      }
    }
  }
}

object ZookeeperTaskPersistence {
  def serviceMemberPath(service: Service, member: ServiceMember, taskName: String) = {
    ZookeeperService.memberDataPath(service.name, member.token) + "/spnl/" + taskName
  }
}

class ZookeeperTaskPersistenceFactory(zkClient: ZookeeperClient) extends TaskPersistenceFactory {
  def createServiceMemberPersistence(service: Service, member: ServiceMember) = {
    new ZookeeperTaskPersistence(zkClient, service, member)
  }
}
