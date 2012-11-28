package com.wajam.spnl

import feeder.Feeder
import org.scalatest.FunSuite
import org.scalatest.matchers.ShouldMatchers._
import org.scalatest.junit.JUnitRunner
import org.junit.runner.RunWith
import com.wajam.nrv.zookeeper.ZookeeperClient
import org.scalatest.mock.MockitoSugar
import com.wajam.nrv.service.{ServiceMember, Service, Action}
import com.wajam.nrv.cluster.LocalNode

@RunWith(classOf[JUnitRunner])
class TestZookeeperTaskPersistence extends FunSuite with MockitoSugar {

  val node = new LocalNode("127.0.0.1", Map("nrv" -> 12345))
  val service = new Service("test_service")
  val localMember = new ServiceMember(1000, node)
  service.addMember(localMember)

  val zkPersistence = new ZookeeperTaskPersistence(new ZookeeperClient("127.0.0.1/tests"), service, localMember)
  val mockFeeder = mock[Feeder]
  val mockAction = mock[TaskAction]

  test("should save and load persisted task") {
    var task = new Task("ittest_persistence", mockFeeder, mockAction, persistence = zkPersistence)
    task.context.data += ("test" -> "value")
    zkPersistence.saveTask(task)

    task = new Task("ittest_persistence", mockFeeder, mockAction, persistence = zkPersistence)
    zkPersistence.loadTask(task)

    assert("value".equals(task.context.data.get("test").get))
  }

  test("should not throw an exception when loading unexistant data") {
    val task = new Task("ittest_unset", mockFeeder, mockAction, persistence = zkPersistence)
    zkPersistence.loadTask(task)
    assert(task.context.data.get("test").isEmpty)
  }
}
