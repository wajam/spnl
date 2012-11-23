package com.wajam.spnl

import com.wajam.nrv.service.{MemberStatus, ServiceMember, Service}

/**
 * Created with IntelliJ IDEA.
 * User: manuel
 * Date: 23/11/12
 * Time: 12:03 PM
 * To change this template use File | Settings | File Templates.
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
