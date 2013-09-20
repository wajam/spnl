package com.wajam.spnl

import com.wajam.nrv.service.{Service, ServiceMember}

/**
 * Persists tasks and their datazzz
 */
trait TaskPersistence {
  def saveTask(task: Task)

  def loadTask(task: Task)
}

/**
 * Factory to create task persistence implementation. New methods should be added when new persistence scope is needed.
 */
trait TaskPersistenceFactory {
  def createServiceMemberPersistence(service: Service, member: ServiceMember): TaskPersistence
}
