package com.wajam.spnl

import feeder.Feeder
import org.scalatest.FunSuite
import org.scalatest.Matchers._
import org.scalatest.junit.JUnitRunner
import org.junit.runner.RunWith
import com.wajam.nrv.zookeeper.ZookeeperClient
import org.scalatest.mock.MockitoSugar
import com.wajam.nrv.service.{ServiceMember, Service}
import com.wajam.nrv.cluster.LocalNode
import org.mockito.Mockito._

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
    when(mockAction.name).thenReturn("ittest_persistence")
    val taskSave = new Task(mockFeeder, mockAction, persistence = zkPersistence)
    taskSave.context.data += ("test" -> "value")
    zkPersistence.saveTask(taskSave)

    val taskLoad = new Task(mockFeeder, mockAction, persistence = zkPersistence)
    zkPersistence.loadTask(taskLoad)

    taskLoad.context.data should be(taskSave.context.data)
  }

  test("should not throw an exception when loading unexistant data") {
    when(mockFeeder.name).thenReturn("ittest_unset")
    val task = new Task(mockFeeder, mockAction, persistence = zkPersistence)
    zkPersistence.loadTask(task)
    assert(task.context.data.get("test").isEmpty)
  }
}
