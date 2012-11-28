package com.wajam.spnl

import com.wajam.nrv.service.{MemberStatus, ServiceMember, Service}

/**
 * Verify if the specified record data should be processed by this task
 */
class TaskAcceptor {
  def accept(data: Map[String, Any]) = true
}

class ServiceMemberTaskAcceptor(service: Service, member: ServiceMember) extends TaskAcceptor {
  override def accept(data: Map[String, Any]) = {
    data.get("token") match {
      case Some(token: String) => {
        service.resolveMembers(token.toLong, 1).forall(resolved => resolved == member && resolved.status == MemberStatus.Up)
      }
      case _ => false // TODO: error if no token?
    }
  }
}
