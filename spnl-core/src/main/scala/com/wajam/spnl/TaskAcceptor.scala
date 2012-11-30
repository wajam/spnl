package com.wajam.spnl

import com.wajam.nrv.service.{MemberStatus, ServiceMember, Service}

/**
 * Verify if the specified record data should be processed by this task
 */
trait TaskAcceptor {
  def accept(data: Map[String, Any]): Boolean
}

class AcceptAllTaskAcceptor extends TaskAcceptor {
  def accept(data: Map[String, Any]) = true
}

class ServiceMemberTaskAcceptor(service: Service, member: ServiceMember) extends TaskAcceptor {
  override def accept(data: Map[String, Any]) = {
    data.get("token") match {
      case Some(token: String) => {
        service.resolveMembers(token.toLong, 1).forall(resolved => resolved == member && resolved.status == MemberStatus.Up)
      }
      case _ => throw new Exception("Task data is missing token")
    }
  }
}
