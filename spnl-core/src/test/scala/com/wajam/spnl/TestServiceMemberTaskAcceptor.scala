package com.wajam.spnl

import org.scalatest.FunSuite
import com.wajam.nrv.cluster.{Cluster, StaticClusterManager, Node, LocalNode}
import com.wajam.nrv.service.{ServiceMember, Service}
import org.scalatest.matchers.ShouldMatchers._

class TestServiceMemberTaskAcceptor extends FunSuite {

  test("should only accept when expected") {
    // Setup cluster
    val node1 = new LocalNode("127.0.0.1", Map("nrv" -> 12345))
    val node2 = new Node("127.0.0.2", Map("nrv" -> 12345))
    val clusterManager = new StaticClusterManager
    val cluster = new Cluster(node1, clusterManager)

    val service = new Service("test_service")
    cluster.registerService(service)
    val localMember1 = new ServiceMember(1000, node1)
    val localMember2 = new ServiceMember(2000, node1)
    val remoteMember = new ServiceMember(3000, node2)
    service.addMember(localMember1)
    service.addMember(localMember2)
    service.addMember(remoteMember)

    // Should not accept if expected member is down
    val acceptor = new ServiceMemberTaskAcceptor(service, localMember1)
    acceptor.accept(Map("token" -> "500")) should be (false)
    acceptor.accept(Map("token" -> "3500")) should be (false)

    // Should accept if expected member is up
    cluster.start()
    acceptor.accept(Map("token" -> "500")) should be (true)
    acceptor.accept(Map("token" -> "3500")) should be (true)

    // Same node but other member token
    acceptor.accept(Map("token" -> "1500")) should be (false)

    // Other node
    acceptor.accept(Map("token" -> "2500")) should be (false)

    // No token!!!
    evaluating {
      acceptor.accept(Map())
    } should produce [Exception]
  }
}
