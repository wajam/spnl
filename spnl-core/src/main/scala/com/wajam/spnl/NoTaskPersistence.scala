package com.wajam.spnl

import com.wajam.nrv.service.{ServiceMember, Service}

/**
 * Doesn't persist any task, even if they aren't ephemeral
 */
object NoTaskPersistence extends TaskPersistence {
  def saveTask(task: Task) {}

  def loadTask(task: Task) {}
}

class NoTaskPersistenceFactory extends TaskPersistenceFactory {
  def createServiceMemberPersistence(service: Service, member: ServiceMember) = NoTaskPersistence
}
